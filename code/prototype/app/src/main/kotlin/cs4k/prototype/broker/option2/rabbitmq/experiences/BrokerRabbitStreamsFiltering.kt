package cs4k.prototype.broker.option2.rabbitmq.experiences

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Address
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BaseSubscriber
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.SubscriberWithEventTracking
import cs4k.prototype.broker.option2.rabbitmq.ChannelPool
import cs4k.prototype.broker.option2.rabbitmq.experiences.OffsetShareMessage.OffsetShareRequestType.REQUEST
import cs4k.prototype.broker.option2.rabbitmq.experiences.OffsetShareMessage.OffsetShareRequestType.RESPONSE
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Collections
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// [NOTE] Discontinued, mainly, because:
//      - Consuming with several filters requires several channels, which in turn uses up several resources.
//      - While consumer can be restarted, it may introduce overhead when several new topics are subscribed to at once.

// - RabbitMQ Java client
// - RabbitMQ Streams (1 stream - n consumers (using filters))
// - Support for RabbitMQ Cluster

// @Component
class BrokerRabbitStreamsFiltering(
    private val subscribeDelay: Duration = 250.milliseconds,
    private val withholdTimeInMillis: Long = 5000
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

    // Executor in charge of cleaning unheard consumers.
    private val cleanExecutor = Executors.newSingleThreadScheduledExecutor()

    // Storage for topics that are consumed, storing channel, last offset and last event.
    private val consumedTopics = ConsumedTopicsDepreciated()

    /**
     * Requesting an offset from surrounding brokers to consume from common stream.
     * @param topic The topic that consumer wants to consume.
     */
    private fun fetchOffset(topic: String) {
        val publishingChannel = publishingChannelPool.getChannel()
        val request = OffsetShareMessage.serialize(OffsetShareRequest(brokerId, topic).toMessage()).toByteArray()
        publishingChannel.basicPublish(offsetExchange, "", null, request)
        publishingChannelPool.stopUsingChannel(publishingChannel)
    }

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumingChannelPool.isClosed || consumingChannelPool.isClosed))
    }

    /**
     * Consumer used to process messages from stream.
     * @param topic The topic that the consumer wants to consume.
     * @param channel The channel where messages are sent to.
     */
    private inner class BrokerConsumer(val topic: String, channel: Channel) : DefaultConsumer(channel) {

        /**
         * Registers the event as the latest received and sends it to all subscribers.
         */
        private fun processMessage(offset: Long, payload: String) {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            val eventToNotify = consumedTopics.createAndSetLatestEvent(topic, offset, message, isLast)
            consumedTopics.setLatestMessageAccessInfoIfLatest(topic, offset, eventToNotify.id)
            logger.info("event received from stream -> {}", eventToNotify)
            associatedSubscribers
                .getAll(topic)
                .forEach { subscriber ->
                    if ((subscriber as SubscriberWithEventTracking).lastEventIdReceived < eventToNotify.id) {
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
            val offset = properties.headers["x-stream-offset"].toString().toLong()
            val storedOffset = consumedTopics.getOffsetNoWait(topic)
            if (storedOffset == null || offset > storedOffset) {
                val payload = String(body)
                processMessage(offset, payload)
            }
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    /**
     * Consumer used to receive requests for offsets and sends them out as responses.
     * @param channel Channel where requests come from.
     */
    private inner class OffsetShareHandler(channel: Channel) : DefaultConsumer(channel) {

        private fun handleRequest(request: OffsetShareRequest) {
            val topic = request.topic
            val offset = consumedTopics.getOffsetNoWait(topic)
            if (request.senderQueue != brokerId && offset != null) {
                val publishChannel = publishingChannelPool.getChannel()
                val accessInfo = consumedTopics.getLatestMessageAccessInfo(topic)
                val response = OffsetShareMessage.serialize(
                    OffsetShareResponse(
                        accessInfo?.offset ?: 0L,
                        accessInfo?.lastEventId ?: 0L,
                        topic
                    ).toMessage()
                ).toByteArray()
                publishChannel.basicPublish(
                    "",
                    request.senderQueue,
                    null,
                    response
                )
                consumingChannelPool.stopUsingChannel(publishChannel)
            }
        }

        private fun handleResponse(response: OffsetShareResponse) {
            consumedTopics
                .setLatestMessageAccessInfoIfLatest(response.topic, response.offset, response.lastEventId)
        }

        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            val message = OffsetShareMessage.deserialize(String(body))
            when (message.type) {
                REQUEST -> handleRequest(message.toRequest())
                RESPONSE -> handleResponse(message.toResponse())
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
                    "x-max-length-bytes" to 100_000
                )
            )
            consumingChannelPool.stopUsingChannel(consumingChannel)
        }, retryCondition)
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
            channel.exchangeDeclare(offsetExchange, "direct")
            val consumerTag = channel.basicConsume(brokerId, OffsetShareHandler(channel))
            consumingChannelPool.registerConsuming(channel, consumerTag)
        }, retryCondition)
    }

    init {
        createStream()
        enableOffsetSharing()
    }

    // Flag that indicates if broker is gracefully shutting down.
    private val isShutdown = AtomicBoolean(false)

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        val subscriber = SubscriberWithEventTracking(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) { listen(topic) }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)
        getLastEvent(topic)?.let { event ->
            if (subscriber.lastEventIdReceived < event.id) {
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
    private fun unsubscribe(topic: String, subscriber: SubscriberWithEventTracking) {
        val topicLock = consumedTopics.getLock(topic)
        associatedSubscribers.removeIf(
            topic,
            { sub: BaseSubscriber -> sub.id.toString() == subscriber.id.toString() },
            {
                if (consumedTopics.markForAnalysis(topic)) {
                    cleanExecutor.schedule({
                        try {
                            topicLock?.lock()
                            unListen(topic)
                            consumedTopics.stopAnalysis(topic)
                        } finally {
                            topicLock?.unlock()
                        }
                    }, withholdTimeInMillis, TimeUnit.MILLISECONDS)
                }
            }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        notify(topic, message, isLastMessage)
    }

    /**
     * Listen for notifications.
     */
    private fun listen(topic: String) {
        val topicLock = consumedTopics.getLock(topic)
        try {
            topicLock?.lock()
            if (consumedTopics.isTopicBeingConsumed(topic) || consumedTopics.isBeingAnalyzed(topic)) return
            val offset = consumedTopics.getOffset(topic, subscribeDelay) { fetchOffset(topic) } ?: "first"
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                val consumingChannel = consumingChannelPool.getChannel()
                consumedTopics.setChannel(topic, consumingChannel)
                consumingChannel.basicQos(100)
                val consumerTag = consumingChannel.basicConsume(
                    streamName,
                    false,
                    mapOf(
                        "x-stream-offset" to offset,
                        "x-stream-filter" to topic
                    ),
                    BrokerConsumer(topic, consumingChannel)
                )
                consumingChannelPool.registerConsuming(consumingChannel, consumerTag)
                consumingChannel.queueBind(brokerId, offsetExchange, topic)
                logger.info("new consumer -> {}", topic)
            }, retryCondition)
        } finally {
            topicLock?.unlock()
        }
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
     * Gracefully letting go of resources related to the topic.
     * @param topic The topic formerly consumed.
     */
    private fun unListen(topic: String) {
        if (associatedSubscribers.noSubscribers(topic)) {
            logger.info("consumer for topic {} closing", topic)
            consumedTopics.getChannel(topic)?.let { channel ->
                retryExecutor.execute({ BrokerLostConnectionException() }, {
                    channel.queueUnbind(brokerId, offsetExchange, topic)
                    consumingChannelPool.stopUsingChannel(channel)
                }, retryCondition)
            }
            consumedTopics.removeTopic(topic)
        }
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
            cleanExecutor.shutdown()
            consumingChannelPool.getChannel().exchangeDelete(offsetExchange)
            consumingChannelPool.getChannel().queueDelete(brokerId)
            publishingChannelPool.close()
            consumingChannelPool.close()
            consumedTopics.removeAll()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitStreamsFiltering::class.java)

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
