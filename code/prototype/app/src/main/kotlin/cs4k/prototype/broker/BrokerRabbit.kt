package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.stream.ByteCapacity
import com.rabbitmq.stream.ConfirmationStatus
import com.rabbitmq.stream.Consumer
import com.rabbitmq.stream.Environment
import com.rabbitmq.stream.Message
import com.rabbitmq.stream.MessageHandler
import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.StreamException
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

// @Component
class BrokerRabbit {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Association between topics and consumers.
    private val topicConsumers = TopicConsumers()

    // Association between topics and producers.
    private val topicProducers = TopicProducers()

    // Collection of the most recent events - sent and received.
    private val latestTopicEvents = LatestTopicEvents()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Prefix of the created streams.
    private val streamsNamePrefix = "cs4k-notifications:"

    // Environment used to connect to message broker
    private val environment = createEnvironment()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        throwable is StreamException
    }

    private fun messageToEvent(context: MessageHandler.Context?, message: Message?): Event {
        requireNotNull(context)
        requireNotNull(message)
        val offset = context.offset()
        val body = message.bodyAsBinary
        return deserialize(String(body)).copy(id = offset)
    }

    /**
     * Object that handles notifications.
     */
    private val handler = MessageHandler { context, message ->
        val event = messageToEvent(context, message)
        latestTopicEvents.setLatestReceivedEvent(event.topic, event)
        logger.info("received message -> {}", event.toString())
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    /**
     * Creates a new stream for a given topic.
     */
    private fun createStream(topic: String) {
        environment.streamCreator()
            .stream(streamsNamePrefix + topic)
            .maxLengthBytes(ByteCapacity.B(MAX_BYTES))
            .create()
    }

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The callback to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            listen(topic)
        }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            logger.info("shutting down...")
            topicProducers.getAllTopics().forEach { topic ->
                topicProducers.getProducer(topic)?.close()
                topicProducers.removeProducer(topic)
            }
            topicConsumers.getAllTopics().forEach { topic ->
                unListen(topic)
            }
            environment.close()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(
            topic,
            predicate = { sub -> sub.id == subscriber.id },
            onTopicRemove = { unListen(topic) }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Listen for notifications.
     */
    private fun listen(topic: String) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            createStream(topic)
            val consumer = environment.consumerBuilder()
                .stream(streamsNamePrefix + topic)
                .offset(OffsetSpecification.last())
                .messageHandler(handler)
                .build()
            topicConsumers.setConsumer(topic, consumer)
        }, retryCondition)
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen(topic: String) {
        val consumer = topicConsumers.getConsumer(topic)
        consumer?.let { c ->
            c.close()
            topicConsumers.removeConsumer(topic)
        }
    }

    /**
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param text The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(topic: String, text: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            createStream(topic)
            var producer = topicProducers.getProducer(topic)
            var isFailed = false
            if (producer == null) {
                producer = environment.producerBuilder()
                    .stream(streamsNamePrefix + topic)
                    .build()
                topicProducers.setProducer(topic, producer)
            }
            val event = Event(topic, -1, text, isLastMessage)
            producer?.let {
                val message = producer.messageBuilder()
                    .addData(serialize(event).toByteArray())
                    .build()
                val latch = CountDownLatch(1)
                producer.send(message) { confirmStatus ->
                    if(confirmStatus.isConfirmed) {
                        latestTopicEvents.setLatestSentEvent(topic, event)
                    }
                    else {
                        isFailed = true
                    }
                    latch.countDown()
                }
                latch.await()
                if(isFailed)
                    throw StreamException("message failed to be delivered.")
            }
        }, retryCondition)
    }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? {
        var consumer: Consumer? = null
        val lastEvent = runBlocking {
            withTimeoutOrNull(100) {
                suspendCancellableCoroutine { continuation ->
                    createStream(topic)
                    retryExecutor.execute({ BrokerDbLostConnectionException() }, {
                        consumer = environment.consumerBuilder()
                            .stream(streamsNamePrefix + topic)
                            .messageHandler { context, message ->
                                if(context.offset() >= latestTopicEvents.getLatestEventId(topic))
                                    continuation.resumeWith(Result.success(messageToEvent(context, message)))
                            }
                            .build()
                    }, retryCondition)
                }
            }
        }
        // Closing consumer to release resources.
        consumer?.close()
        return lastEvent
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbit::class.java)

        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        // Max size of a stream in bytes.
        private const val MAX_BYTES = 200_000L

        /**
         * Function that creates an environment for message broker communication.
         */
        private fun createEnvironment() = Environment.builder()
            .host("localhost")
            .port(5552)
            .build()

        /**
         * Serialize an event to JSON string.
         *
         * @param event The event to serialize.
         * @return The resulting JSON string.
         */
        private fun serialize(event: Event) = objectMapper.writeValueAsString(event)

        /**
         * Deserialize a JSON string to event.
         *
         * @param payload The JSON string to deserialize.
         * @return The resulting event.
         */
        private fun deserialize(payload: String) = objectMapper.readValue(payload, Event::class.java)
    }
}
