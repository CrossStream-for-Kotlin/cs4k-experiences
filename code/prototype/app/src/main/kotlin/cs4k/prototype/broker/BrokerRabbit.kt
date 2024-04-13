package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import cs4k.prototype.broker.BrokerException.BrokerDbConnectionException
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.BrokerException.DbConnectionPoolSizeException
import cs4k.prototype.broker.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.ChannelCommandOperation.Listen
import cs4k.prototype.broker.ChannelCommandOperation.UnListen
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.IOException
import java.sql.Connection
import java.sql.SQLException
import java.util.UUID
import kotlin.concurrent.thread

@Component
class BrokerRabbit {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Name of exchange used to publish messages to.
    private val exchangeName = "cs4k-notifications"

    // Consumer tag identifying the broker as consumer.
    private val consumerTag = "cs4k-broker:" + UUID.randomUUID().toString()

    // Channel used for notifications.
    private val acceptingChannel = connectionFactory.newConnection().createChannel()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && !acceptingChannel.isOpen)
    }

    private inner class BrokerConsumer: DefaultConsumer(acceptingChannel) {

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            val deliveryTag = envelope.deliveryTag
            val event = deserialize(String(body)).copy(id = deliveryTag - 1)
            associatedSubscribers
                .getAll(event.topic)
                .forEach { subscriber -> subscriber.handler(event) }
            acceptingChannel.basicAck(deliveryTag, false)
        }
    }

    // Single instance of consumer in broker
    private val consumer = BrokerConsumer()

    init {
        // Start a new thread to listen for notifications.
        thread {
            // Listen for notifications.
            listen()
        }
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
        if (!acceptingChannel.isOpen) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
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
        if (!acceptingChannel.isOpen) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

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

        unListen()
        acceptingChannel.close()
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic) { sub -> sub.id == subscriber.id }
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Listen for notifications.
     */
    private fun listen() {
        acceptingChannel.exchangeDeclare(exchangeName, "fanout")
        val queueName = acceptingChannel.queueDeclare().queue
        acceptingChannel.queueBind(queueName, exchangeName, "")
        acceptingChannel.basicConsume(queueName, false, consumerTag, consumer)
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen() = acceptingChannel.basicCancel(consumerTag)

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
                channel.exchangeDeclare(exchangeName, "fanout")
                val event = Event(topic, -1, message, isLastMessage)
                channel.basicPublish(exchangeName, "", null, serialize(event).toByteArray())
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
