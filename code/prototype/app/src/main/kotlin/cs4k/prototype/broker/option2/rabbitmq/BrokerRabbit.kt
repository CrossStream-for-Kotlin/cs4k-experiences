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
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.option2.rabbitmq.HistoryShareMessage.HistoryShareMessageType.REQUEST
import cs4k.prototype.broker.option2.rabbitmq.HistoryShareMessage.HistoryShareMessageType.RESPONSE
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds

class BrokerRabbit(
    private val subscribeDelayInMillis: Long = DEFAULT_SUBSCRIBE_DELAY_MILLIS
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
    private val historyExchange = "cs4k-history-exchange"

    // Storage for topics that are consumed, storing channel, last offset and last event.
    private val consumedTopics = ConsumedTopics()

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    private inner class BrokerConsumer(channel: Channel) : DefaultConsumer(channel) {

        private fun processMessage(message: Message, offset: Long) {
            val event = consumedTopics.createAndSetLatestEventAndOffset(
                message.topic,
                offset,
                message.message,
                message.isLast
            )
            logger.info("event received -> {}", event)
            associatedSubscribers.getAll(event.topic)
                .forEach { it.handler(event) }
        }

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(properties)
            requireNotNull(body)
            val message = Message.deserialize(String(body))
            val offset = properties.headers["x-stream-offset"].toString().toLong()
            processMessage(message, offset)
            channel.basicAck(envelope.deliveryTag, false)
        }
    }

    /**
     * Consumer used to receive requests for offsets and IDs and sends them out as responses.
     * @param channel Channel where requests come from.
     */
    private inner class HistoryShareHandler(channel: Channel) : DefaultConsumer(channel) {

        private val gotInfoFromPeer = AtomicBoolean(false)

        private fun handleRequest(request: HistoryShareRequest) {
            logger.info("received request from {}", request.senderQueue)
            val publishChannel = publishingChannelPool.getChannel()
            val accessInfo = consumedTopics.getAllLatestEventInfos()
            val response = HistoryShareResponse(accessInfo).toHistoryShareMessage()
            publishChannel.basicPublish(
                "",
                request.senderQueue,
                null,
                response.toString().toByteArray()
            )
            publishingChannelPool.stopUsingChannel(publishChannel)
        }

        private fun handleResponse(response: HistoryShareResponse) {
            if (gotInfoFromPeer.compareAndSet(false, true)) {
                logger.info("received response, storing...")
                consumedTopics.fullUpdate(response.allConsumeInfo)
                channel.queueBind(brokerId, historyExchange, "")
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
            val message = HistoryShareMessage.deserialize(String(body))
            when (message.type) {
                REQUEST -> handleRequest(message.toRequest())
                RESPONSE -> handleResponse(message.toResponse())
            }
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    init {
        createStream()
        createControlQueue()
        createHistoryExchange()
        fetchStoredInfoFromPeers()
        listen()
    }

    private fun createStream() {
        val channel = publishingChannelPool.getChannel()
        channel.queueDeclare(
            streamName,
            true,
            false,
            false,
            mapOf(
                "x-queue-type" to "stream",
                "x-max-length-bytes" to DEFAULT_STREAM_SIZE
            )
        )
        publishingChannelPool.stopUsingChannel(channel)
    }

    private fun listen() {
        val offset = consumedTopics.getMaximumOffset(subscribeDelayInMillis.milliseconds) ?: "last"
        val channel = consumingChannelPool.getChannel()
        channel.basicQos(DEFAULT_PREFETCH_VALUE)
        channel.basicConsume(
            streamName,
            false,
            mapOf(
                "x-stream-offset" to offset
            ),
            BrokerConsumer(channel)
        )
    }

    private fun createHistoryExchange() {
        val channel = publishingChannelPool.getChannel()
        channel.exchangeDeclare(
            historyExchange,
            "fanout"
        )
        publishingChannelPool.stopUsingChannel(channel)
    }

    private fun createControlQueue() {
        val channel = consumingChannelPool.getChannel()
        channel.queueDeclare(
            brokerId,
            true,
            false,
            false,
            null
        )
        channel.basicConsume(brokerId, HistoryShareHandler(channel))
    }

    /**
     * Requesting stored info surrounding brokers to consume from common stream.
     */
    private fun fetchStoredInfoFromPeers() {
        val publishingChannel = publishingChannelPool.getChannel()
        val request = HistoryShareRequest(brokerId).toHistoryShareMessage()
        publishingChannel.basicPublish(historyExchange, "", null, request.toString().toByteArray())
        logger.info("started up - sending request to obtain info")
        publishingChannelPool.stopUsingChannel(publishingChannel)
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}'", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    private fun getLastEvent(topic: String) = consumedTopics.getLatestEvent(topic)

    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { it.id.toString() == subscriber.id.toString() })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        val body = Message.serialize(Message(topic, message, isLastMessage)).toByteArray()
        val channel = publishingChannelPool.getChannel()
        channel.basicPublish("", streamName, null, body)
        publishingChannelPool.stopUsingChannel(channel)
    }

    override fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            consumingChannelPool.close()
            publishingChannelPool.close()
            consumedTopics.removeAll()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbit::class.java)

        private const val DEFAULT_PREFETCH_VALUE = 100
        private const val DEFAULT_SUBSCRIBE_DELAY_MILLIS = 250L
        private const val DEFAULT_STREAM_SIZE = 8_000_000

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
    }
}
