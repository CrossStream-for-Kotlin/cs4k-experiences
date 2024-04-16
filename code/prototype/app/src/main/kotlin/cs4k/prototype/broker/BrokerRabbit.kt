package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*

//@Component
class BrokerRabbit {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Association between topics and consumer tags.
    private val topicConsumers = TopicConsumers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Name of exchange used to publish messages to.
    private val exchangeName = "cs4k-notifications"

    // Channel used for notifications.
    private val acceptingChannel = connectionFactory.newConnection().createChannel()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && !acceptingChannel.isOpen)
    }

    private inner class TopicConsumer(val topic: String) : DefaultConsumer(acceptingChannel) {

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(properties)
            requireNotNull(envelope)
            requireNotNull(body)
            if (topic == properties.headers["x-stream-filter-value"].toString()) {
                val deliveryTag = envelope.deliveryTag
                val event = deserialize(String(body)).copy(id = deliveryTag - 1)
                val newOffset = properties.headers["x-stream-offset"].toString().toLong()
                associatedSubscribers
                    .getAll(event.topic)
                    .forEach { subscriber -> subscriber.handler(event) }
                acceptingChannel.basicAck(deliveryTag, false)
                topicConsumers.setTopicOffset(topic, newOffset)
            }
        }
    }

    private var isShutdown = false

    private fun createStream() {
        acceptingChannel.queueDeclare(
            exchangeName,
            true,
            false,
            false,
            mapOf(
                "x-queue-type" to "stream",
                "x-max-length-bytes" to MAX_BYTES
            )
        )
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
        if (isShutdown || !acceptingChannel.isOpen) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            listen(topic)
        }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

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
        if (isShutdown || !acceptingChannel.isOpen) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (!acceptingChannel.isOpen) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        isShutdown = true
        topicConsumers.forEachConsumedTopic {
            unListen(it)
        }
        acceptingChannel.close()
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
        createStream()
        acceptingChannel.basicQos(100)
        val offset = (topicConsumers.getOffset(topic) ?: 0L)
        val consumerTag = acceptingChannel.basicConsume(
            exchangeName,
            false,
            mapOf(
                "x-stream-offset" to offset,
                "x-stream-filter" to topic
            ),
            TopicConsumer(topic)
        )
        topicConsumers.setTopic(topic, consumerTag, offset)
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen(topic: String) {
        val consumerTag = topicConsumers.getConsumerTag(topic)
        acceptingChannel.basicCancel(consumerTag)
        topicConsumers.removeConsumerTag(topic)
    }

    /**
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionFactory.newConnection().use {
                val channel = it.createChannel()
                val event = Event(topic, -1, message, isLastMessage)
                channel.basicPublish(
                    "",
                    exchangeName,
                    AMQP.BasicProperties.Builder()
                        .headers(
                            mapOf("x-stream-filter-value" to topic)
                        ).build(),
                    serialize(event).toByteArray()
                )
            }
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbit::class.java)

        // Connection factory used to make connections to message broker.
        private val connectionFactory = ConnectionFactory()

        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private const val MAX_BYTES = 200_000_000

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
