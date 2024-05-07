package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.common.BrokerException.*
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
//import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.Continuation
import kotlin.time.Duration.Companion.milliseconds

// @Component
class BrokerRabbitQueues {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    /**
     * Cria um evento especial para solicitar o último evento de um tópico específico a outro broker.
     *
     * @param message A mensagem para o evento de solicitação.
     * @return Um evento configurado como pedido de último evento.
     */
    private fun createRequestEvent(topic: String): Event {
        return Event(topic, -1, "Request Latest Event", false)
    }

    // Name of exchange used to publish messages to.
    private val broadCastExchange = "cs4k-notifications"

    //Indetify Broker instace
    private val brokerNumber = UUID.randomUUID().toString()

    // Consumer tag identifying the broker as consumer.
    private val consumerTag = "cs4k-broker:" + UUID.randomUUID().toString()




    // Channel pool
    private val consumerChannelPool = ChannelPool(connectionFactory.newConnection())
    private val publisherChannelPool = ChannelPool(connectionFactory.newConnection())



    // Storage of most recent events sent by other brokers and set by this broker.
    private val latestTopicEvents = LatestTopicEvents()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumerChannelPool.isClosed || consumerChannelPool.isClosed))
    }

    /**
     * Registers the event as the latest received and sends it to all subscribers.
     */
    private fun processMessage(event: Event, toResend: Boolean = false) {
        latestTopicEvents.setLatestReceivedEvent(event.topic, event)
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber ->
                if (subscriber.lastEventId < event.id) {
                    associatedSubscribers.updateLastEventListened(subscriber.id, event.topic, event.id)
                    subscriber.handler(event)
                }
            }
        if (toResend) {
            notify(event)
        }
    }

    /**
     * Checks if there is an id conflict between two different events.
     */
    private fun thereIsIdConflict(latestEvent: Event, event: Event) =
        latestEvent.id == event.id &&
                (latestEvent.message != event.message )

    private inner class ConsumerHandler(channel: Channel) : DefaultConsumer(channel) {

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            val deliveryTag = envelope.deliveryTag
            val event = deserialize(String(body))
            val latestEvent = latestTopicEvents.getLatestReceivedEvent(event.topic)
            if (event.message == "Request Latest Event") {
                logger.info(
                    "Request Latest Event received to routinkg key {}  in the consumertag {} and exchange {}",
                    envelope.routingKey,
                    consumerTag,
                    envelope.exchange
                )
                if (latestEvent != null){
                    channel.basicPublish(
                        broadCastExchange, latestEvent.topic,
                        null,
                        serialize(latestEvent).toByteArray()
                    )
                    channel.basicAck(deliveryTag, false)
                }
                return
            }

            logger.info(
                "event received message {} with id {} in broker {} ",
                event.message,
                event.id,
                brokerNumber
            )
            when {

                latestEvent != null && thereIsIdConflict(latestEvent, event) -> {
                    val recentEvent = event.copy(id = event.id + 1)
                    logger.info("same event received with different id, latest updated {}", recentEvent)
                    processMessage(recentEvent, true)
                }

                latestEvent == null || latestEvent.id < event.id -> {
                    logger.info("new event received {}", event)
                    processMessage(event)
                }

                latestEvent == event -> {
                    logger.info("same event received, thrown away")
                }

                latestEvent.id > event.id -> {
                    logger.info("older event received, thrown away")
                }
            }
            channel.basicAck(deliveryTag, false)
        }
    }

    init {
        listen()
    }

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The callback to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{} in broker {}", topic, subscriber.id, brokerNumber)
        val handler = getLastEvent(topic)
        if (handler != null) {
            subscriber.handler(handler)
        } else {
             //requestLatestEvent(topic)
        }

        return { unsubscribe(topic, subscriber) }
    }


    

    private fun getLastEvent(topic: String): Event? {
        val event = latestTopicEvents.getLatestEvent(topic)
        logger.info("last event received -> {}", event)
        return event
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        logger.info("publishing message to topic '{} with message {} in broker {}'", topic, message, brokerNumber)
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        logger.info("Shutting down broker")
        if (isShutdown.compareAndSet(false, true)) {
            consumerChannelPool.close()
            publisherChannelPool.close()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private fun requestLatestEvent(topic: String) {
        val requestEvent = Event(topic, -1, "Request Latest Event", false)
        val responseChannel = consumerChannelPool.getChannel()
        responseChannel.confirmSelect()
        //val qName = responseChannel.queueDeclare().queue
        responseChannel.basicPublish(broadCastExchange, "", null, serialize(requestEvent).toByteArray())
        //responseChannel.basicConsume(qName, true, consumerTag, ConsumerHandler(responseChannel))
        responseChannel.waitForConfirms()
        //responseChannel.queueDelete(qName)
        consumerChannelPool.stopUsingChannel(responseChannel)
    }


    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Listen for notifications.
     */
    private fun listen() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = consumerChannelPool.getChannel()
            channel.exchangeDeclare(broadCastExchange, "fanout")
            channel.queueDeclare(
                consumerTag,
                true,
                false,
                false,
                mapOf("x-max-length" to 100_000, "x-queue-type" to "quorum")
            )
            logger.info("queue declared with tag {}", consumerTag)
            channel.queueBind(consumerTag, broadCastExchange, "")
            channel.basicConsume(consumerTag, false, consumerTag, ConsumerHandler(channel))
            consumerChannelPool.registerConsuming(channel, consumerTag)
        }, retryCondition)
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen() {
        consumerChannelPool.close()
    }

    /**
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute(exception = { BrokerLostConnectionException() }, action = {
            val channel = publisherChannelPool.getChannel()
            channel.confirmSelect()
            val id = latestTopicEvents.getNextEventId(topic)
            val event = Event(topic, id, message, isLastMessage)
            latestTopicEvents.setLatestSentEvent(topic, event)
            channel.basicPublish(broadCastExchange, topic, null, serialize(event).toByteArray())
            logger.info("Message successfully published to topic: $topic with message: $message")
            channel.waitForConfirms()
            publisherChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    /**
     * Notify the topic with the event.
     *
     * @param event The event to be sent.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(event: Event) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = publisherChannelPool.getChannel()
            channel.confirmSelect()
            latestTopicEvents.setLatestSentEvent(event.topic, event)
            channel.basicPublish(broadCastExchange, event.topic, null, serialize(event).toByteArray())
            channel.waitForConfirms()
            publisherChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitQueues::class.java)

        // Connection factory used to make connections to message broker.
        private val connectionFactory = ConnectionFactory().apply {
            username = "user"
            password = "password"
        }

        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

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

