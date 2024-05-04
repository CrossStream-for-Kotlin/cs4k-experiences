package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Collections
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.Continuation
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

// @Component
class BrokerRabbitStreams(
    private val subscribeDelay: Duration = 250.milliseconds,
    private val withholdTimeInMillis: Long = 5000
) {
    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Channel pool
    private val channelPool = ChannelPool(connectionFactory.newConnection())

    // Name of stream used to publish messages to.
    private val streamName = "cs4k-notifications"

    // ID of broker used as name for the queue to receive offset requests.
    private val brokerId = "cs4k-broker:" + UUID.randomUUID().toString()

    // Exchange used to send offset requests to.
    private val offsetExchange = "cs4k-offset-exchange"

    // Executor in charge of cleaning unheard consumers.
    private val cleanExecutor = Executors.newSingleThreadScheduledExecutor()

    // Storages for consuming channels and latest offsets and events.
    private val consumeChannelStore = ConsumeChannelStore()
    private val latestEventStore = LatestEventStore()
    private val latestOffsetFetcher = LatestOffsetFetcher { topic -> fetchOffset(topic) }

    /**
     * Requesting an offset from surrounding brokers to consume from common stream.
     */
    private fun fetchOffset(topic: String): Long {
        val channel = channelPool.getChannel()
        val offset = runBlocking {
            withTimeoutOrNull(400) {
                suspendCancellableCoroutine { continuation ->
                    val queue = channel.queueDeclare().queue
                    val request = OffsetSharingRequest(brokerId, queue, topic)
                    channel.basicPublish(offsetExchange, "", null, request.toString().toByteArray())
                    val consumerTag = channel.basicConsume(
                        queue,
                        true,
                        emptyMap(),
                        OffsetReceiverHandler(continuation, channel)
                    )
                    channelPool.registerConsuming(channel, consumerTag)
                }
            }
        } ?: 0L
        channelPool.stopUsingChannel(channel)
        return offset
    }

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (channelPool.isClosed || channelPool.isClosed))
    }

    /**
     * Consumer used to process messages from stream.
     */
    private inner class BrokerConsumer(val topic: String, channel: Channel) : DefaultConsumer(channel) {

        /**
         * Registers the event as the latest received and sends it to all subscribers.
         */
        private fun processMessage(payload: String) {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            val eventToNotify = latestEventStore.createAndSetLatestEvent(topic, message, isLast)
            logger.info("event received -> {}", eventToNotify)
            associatedSubscribers
                .getAll(topic)
                .forEach { subscriber ->
                    if (subscriber.lastEventId < eventToNotify.id) {
                        associatedSubscribers.updateLastEventListened(subscriber.id, topic, eventToNotify.id)
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
            val storedOffset = latestOffsetFetcher.getOffsetNoWait(topic)
            if (filterValue == topic && (storedOffset == null || offset > storedOffset)) {
                latestOffsetFetcher.setOffset(topic, offset)
                val payload = String(body)
                processMessage(payload)
            }
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    /**
     * Consumer used to receive requests for offsets and sends them out as responses.
     */
    private inner class OffsetShareHandler(channel: Channel) : DefaultConsumer(channel) {
        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(envelope)
            requireNotNull(body)
            val request = String(body).toOffsetSharingRequest()
            val topic = request.topic
            val offset = latestOffsetFetcher.getOffsetNoWait(topic)
            if (request.sender != brokerId && offset != null) {
                val publishChannel = channelPool.getChannel()
                publishChannel.basicPublish(
                    "",
                    request.senderQueue,
                    null,
                    offset.toString().toByteArray()
                )
                channelPool.stopUsingChannel(publishChannel)
            }
            val deliveryTag = envelope.deliveryTag
            channel.basicAck(deliveryTag, false)
        }
    }

    /**
     * Consumer responsible for handling responses to offset requests.
     */
    private inner class OffsetReceiverHandler(val continuation: Continuation<Long?>, channel: Channel) :
        DefaultConsumer(channel) {
        val received = AtomicBoolean(false)
        override fun handleDelivery(
            consumerTag: String?,
            envelope: Envelope?,
            properties: AMQP.BasicProperties?,
            body: ByteArray?
        ) {
            requireNotNull(body)
            if (received.compareAndSet(false, true)) {
                continuation.resumeWith(Result.success(String(body).toLong()))
            }
        }
    }

    /**
     * Creates a common stream to receive events.
     */
    private fun createStream() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val consumingChannel = channelPool.getChannel()
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
            channelPool.stopUsingChannel(consumingChannel)
        }, retryCondition)
    }

    /**
     * Creates a queue used to share offsets, binding it to the exchange used to send requests to.
     */
    private fun enableOffsetSharing() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = channelPool.getChannel()
            channel.queueDeclare(
                brokerId,
                false,
                true,
                true,
                mapOf(
                    "x-max-length" to 100
                )
            )
            channel.exchangeDeclare(offsetExchange, "fanout")
            channel.queueBind(brokerId, offsetExchange, "")
            val consumerTag = channel.basicConsume(brokerId, OffsetShareHandler(channel))
            channelPool.registerConsuming(channel, consumerTag)
        }, retryCondition)
    }

    init {
        createStream()
        enableOffsetSharing()
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
        associatedSubscribers.addToKey(topic, subscriber) { listen(topic) }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)
        getLastEvent(topic)?.let { event ->
            if (subscriber.lastEventId < event.id) {
                associatedSubscribers.updateLastEventListened(subscriber.id, topic, event.id)
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
        val channel = consumeChannelStore[topic]
        associatedSubscribers.removeIf(
            topic,
            { sub: Subscriber -> sub.id == subscriber.id },
            {
                if (channel != null && consumeChannelStore.markForAnalysis(topic)) {
                    cleanExecutor.schedule({
                        unListen(topic, channel)
                        consumeChannelStore.stopAnalysis(topic)
                    }, withholdTimeInMillis, TimeUnit.MILLISECONDS)
                }
            }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
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
        notify(topic, message, isLastMessage)
    }

    /**
     * Listen for notifications.
     */
    private fun listen(topic: String) {
        if (consumeChannelStore[topic] != null) return
        val offset = latestOffsetFetcher.getOffset(topic, subscribeDelay)
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val consumingChannel = channelPool.getChannel()
            consumeChannelStore[topic] = consumingChannel
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
            channelPool.registerConsuming(consumingChannel, consumerTag)
            consumeChannelStore[topic] = consumingChannel
        }, retryCondition)
    }

    private fun getLastEvent(topic: String): Event? {
        val event = latestEventStore.getLatestEvent(topic)
        logger.info("last event received -> {}", event)
        return event
    }

    /**
     * Gracefully letting go of resources related to the topic.
     */
    private fun unListen(topic: String, channel: Channel) {
        if (associatedSubscribers.noSubscribers(topic)) {
            consumeChannelStore[topic] = null
            latestOffsetFetcher.removeOffset(topic)
            latestEventStore.removeLatestEvent(topic)
            unListenNow(channel)
        }
    }

    /**
     * UnListen for notifications.
     */
    private fun unListenNow(channel: Channel) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            channelPool.stopUsingChannel(channel)
        }, retryCondition)
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
            val publishingChannel = channelPool.getChannel()
            publishingChannel.basicPublish(
                "",
                streamName,
                AMQP.BasicProperties.Builder()
                    .headers(
                        Collections.singletonMap("x-stream-filter-value", topic) as Map<String, Any>?
                    ).build(),
                payload.toByteArray()
            )
        }, retryCondition)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            cleanExecutor.shutdown()
            latestOffsetFetcher.shutdown()
            channelPool.close()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitStreams::class.java)

        private fun createFactory(): ConnectionFactory {
            val factory = ConnectionFactory()
            return factory
        }

        // Connection factory used to make connections to message broker.
        private val connectionFactory = createFactory()
    }
}
