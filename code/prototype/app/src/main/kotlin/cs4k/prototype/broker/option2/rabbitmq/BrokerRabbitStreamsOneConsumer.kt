package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Address
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

// @Component
class BrokerRabbitStreamsOneConsumer : Broker {
    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Channel pool
    private val consumingChannelPool = ChannelPool(connectionFactory.newConnection(clusterAddresses))
    private val publishingChannelPool = ChannelPool(connectionFactory.newConnection(clusterAddresses))

    // Name of stream used to publish messages to.
    private val streamName = "cs4k-notifications"

    // Storage for topics that are consumed, storing channel, last offset and last event.
    private val eventStore = LatestEventStore()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumingChannelPool.isClosed || consumingChannelPool.isClosed))
    }

    /**
     * Consumer used to process messages from stream.
     * @param topic The topic that the consumer wants to consume.
     * @param channel The channel where messages are sent to.
     */
    private inner class BrokerConsumer(channel: Channel) : DefaultConsumer(channel) {

        /**
         * Registers the event as the latest received and sends it to all subscribers.
         */
        private fun processMessage(topic: String, payload: String) {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            val eventToNotify = eventStore.createAndSetLatestEvent(topic, message, isLast)
            logger.info("event received from stream -> {}", eventToNotify)
            associatedSubscribers
                .getAll(topic)
                .forEach { subscriber ->
                    if (subscriber.lastEventId < eventToNotify.id) {
                        associatedSubscribers.updateLastEventIdListened(subscriber.id, topic, eventToNotify.id)
                        subscriber.handler(eventToNotify)
                    }
                }
        }

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            requireNotNull(properties)
            val filterValue = properties.headers["x-stream-filter-value"].toString()
            val payload = String(body)
            processMessage(filterValue, payload)
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    /**
     * Creates a common stream to receive events.
     */
    private fun createStream() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val consumingChannel = consumingChannelPool.getChannel()
            consumingChannel.queueDeclare(
                streamName,
                true,
                false,
                false,
                mapOf(
                    "x-queue-type" to "stream",
                    "x-max-length-bytes" to 100_000_000
                )
            )
            consumingChannelPool.stopUsingChannel(consumingChannel)
        }, retryCondition)
    }

    /**
     * Listen for notifications.
     */
    private fun listen() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val consumingChannel = consumingChannelPool.getChannel()
            consumingChannel.basicQos(100)
            consumingChannel.basicConsume(
                streamName,
                false,
                mapOf(
                    "x-stream-offset" to "first"
                ),
                BrokerConsumer(consumingChannel)
            )
        }, retryCondition)
    }

    init {
        createStream()
        listen()
    }

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)
        getLastEvent(topic)?.let { event ->
            if (subscriber.lastEventId < event.id) {
                associatedSubscribers.updateLastEventIdListened(subscriber.id, topic, event.id)
                handler(event)
            }
        }
        return { unsubscribe(topic, subscriber) }
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
            { sub: Subscriber -> sub.id.toString() == subscriber.id.toString() },
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        notify(topic, message, isLastMessage)
    }

    /**
     * Obtaining the latest event from topic.
     * @param topic The topic of the event.
     */
    private fun getLastEvent(topic: String): Event? {
        val event = eventStore.getLatestEvent(topic)
        logger.info("last event received from memory -> {}", event)
        return event
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
        val payload = "$message;$isLastMessage"
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val publishingChannel = publishingChannelPool.getChannel()
            publishingChannel.basicPublish(
                "",
                streamName,
                AMQP.BasicProperties.Builder()
                    .headers(
                        Collections.singletonMap("x-stream-filter-value", topic) as Map<String, Any>?
                    ).build(),
                payload.toByteArray()
            )
            logger.info("sent with topic:{} the message: {}", topic, payload)
            publishingChannelPool.stopUsingChannel(publishingChannel)
        }, retryCondition)
    }

    override fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            publishingChannelPool.close()
            consumingChannelPool.getChannel().queueDelete(streamName)
            consumingChannelPool.close()
            eventStore.removeAll()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitStreamsOneConsumer::class.java)

        /**
         * Creates the creator of connections for accessing RabbitMQ broker.
         * @param host The host of the RabbitMQ server
         * @param port The port number to access the RabbitMQ server.
         */
        private fun createFactory(host: String = "localhost", port: Int = 5672): ConnectionFactory {
            val factory = ConnectionFactory()
            factory.username = "user"
            factory.password = "password"
            factory.host = host
            factory.port = port
            return factory
        }

        // Addresses of all nodes inside the cluster.
        private val clusterAddresses = listOf(
            "localhost:5672",
            "localhost:5673",
            "localhost:5674"
        ).map { address ->
            val hostAndPort = address.split(":")
            Address(hostAndPort.dropLast(1).joinToString(":"), hostAndPort.last().toInt())
        }

        // Connection factory used to make connections to message broker.
        private val connectionFactory = createFactory()
    }
}
