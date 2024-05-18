package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Address
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
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
import cs4k.prototype.broker.option2.rabbitmq.OffsetShareMessage.Companion.TYPE_REQUEST
import cs4k.prototype.broker.option2.rabbitmq.OffsetShareMessage.Companion.TYPE_RESPONSE
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// - RabbitMQ Java client
// - RabbitMQ Streams (1 stream - 1 consumer)
// - Support for RabbitMQ Cluster

// @Component
class BrokerRabbitStreams(
    private val subscribeDelay: Duration = 250.milliseconds
) : Broker {
    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Channel pool
    private val consumingChannelPool = ChannelPool(createConnection())
    private val publishingChannelPool = ChannelPool(createConnection())

    // Name of stream used to publish messages to.
    private val streamName = "cs4k-notifications"

    // ID of broker used as name for the queue to receive offset requests.
    private val brokerId = "cs4k-broker:" + UUID.randomUUID().toString()

    // Exchange used to send offset requests to.
    private val offsetExchange = "cs4k-offset-exchange"

    // Storage for topics that are consumed, storing channel, last offset and last event.
    // private val eventStore = LatestEventStore()
    private val consumedTopics = ConsumedTopics()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumingChannelPool.isClosed || consumingChannelPool.isClosed))
    }

    /**
     * Consumer used to process messages from stream.
     * @param channel The channel where messages are sent to.
     */
    private inner class BrokerConsumer(channel: Channel) : DefaultConsumer(channel) {

        /**
         * Registers the event as the latest received and sends it to all subscribers.
         */
        private fun processMessage(topic: String, offset: Long, payload: String) {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            val eventToNotify = consumedTopics.createAndSetLatestEventAndOffset(topic, offset, message, isLast)
            channel.queueBind(brokerId, offsetExchange, "")
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
            val offset = properties.headers["x-stream-offset"].toString().toLong()
            val payload = String(body)
            processMessage(filterValue, offset, payload)
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    /**
     * Consumer used to receive requests for offsets and sends them out as responses.
     * @param channel Channel where requests come from.
     */
    private inner class OffsetShareHandler(channel: Channel) : DefaultConsumer(channel) {

        private val gotInfoFromPeer = AtomicBoolean(false)

        private fun handleRequest(request: HistoryShareRequest) {
            val publishChannel = publishingChannelPool.getChannel()
            val accessInfo = consumedTopics.getAllLatestEventInfos().toTypedArray()
            val response = HistoryShareResponse(accessInfo)
            publishChannel.basicPublish(
                "",
                request.sender,
                null,
                response.toString().toByteArray()
            )
            publishingChannelPool.stopUsingChannel(publishChannel)
        }

        private fun handleResponse(response: HistoryShareResponse) {
            if (gotInfoFromPeer.compareAndSet(false, true)) {
                consumedTopics.fullUpdate(response.events)
                channel.queueBind(brokerId, offsetExchange, "")
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
            val message = String(body).toOffsetShareMessage()
            when (message.type) {
                TYPE_REQUEST -> handleRequest(message.message.toAllOffsetShareRequest())
                TYPE_RESPONSE -> handleResponse(message.message.toAllOffsetShareResponse())
            }
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
            consumingChannel.exchangeDeclare(offsetExchange, "fanout")
            consumingChannelPool.stopUsingChannel(consumingChannel)
        }, retryCondition)
    }

    /**
     * Requesting stored info surrounding brokers to consume from common stream.
     */
    private fun fetchStoredInfoFromPeers() {
        val publishingChannel = publishingChannelPool.getChannel()
        val request = HistoryShareRequest(brokerId)
        publishingChannel.basicPublish(offsetExchange, "", null, request.toString().toByteArray())
        publishingChannelPool.stopUsingChannel(publishingChannel)
    }

    /**
     * Creates a queue used to share offsets, binding it to the exchange used to send requests to.
     */
    private fun enableOffsetSharing() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = consumingChannelPool.getChannel()
            channel.queueDeclare(
                brokerId,
                true,
                false,
                false,
                mapOf(
                    "x-max-length" to 100,
                    "x-queue-type" to "quorum"
                )
            )
            channel.basicConsume(brokerId, OffsetShareHandler(channel))
        }, retryCondition)
    }

    /**
     * Listen for notifications.
     */
    private fun listen() {
        val offset = consumedTopics.getMaximumOffset(subscribeDelay) ?: "first"
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val consumingChannel = consumingChannelPool.getChannel()
            consumingChannel.basicQos(100)
            consumingChannel.basicConsume(
                streamName,
                false,
                mapOf(
                    "x-stream-offset" to offset
                ),
                BrokerConsumer(consumingChannel)
            )
        }, retryCondition)
    }

    init {
        createStream()
        fetchStoredInfoFromPeers()
        enableOffsetSharing()
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
            { sub: Subscriber -> sub.id.toString() == subscriber.id.toString() }
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
        val event = consumedTopics.getLatestEvent(topic)
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
            consumedTopics.removeAll()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitStreams::class.java)

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

        private fun createSingleConnection() = createFactory().newConnection()

        private fun createClusterConnection() = createFactory().newConnection(clusterAddresses)

        private fun createConnection(): Connection {
            // return createSingleConnection()
            return createClusterConnection()
        }

        // Connection factory used to make connections to message broker.
        private val connectionFactory = createFactory()
    }
}
