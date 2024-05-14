package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

// [NOTE] Discontinued, mainly, because:
//      - Cannot garantee that all nodes will have the same id for the same event.
//      - Cenario of multiple publishers and multiple brokers the events can be received 2 or more times.
//      - Possible deadlocks when having multiple brokers.
//      - Possible data loss when having multiple brokers.
//      - All queues binded to the exchange will receive the message even though they don't have subscribers to the topic.

// - RabbitMQ Java client
// - RabbitMQ Queues (1 exchange - n queues)
// - Exchange of type fanout and queues of type quorum
// - Support for RabbitMQ Cluster
class BrokerRabbitFanoutExchange : Broker {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Admin event to request the latest event.
    private val adminTopic = "Request Latest Event"

    // Name of exchange used to publish messages to.
    private val broadCastExchange = "cs4k-notifications"

    // Indetify Broker instace
    private val brokerNumber = UUID.randomUUID().toString()

    // Consumer tag identifying the broker as consumer.
    private val consumerTag = "cs4k-broker:" + UUID.randomUUID().toString()

    // Channel pool for consuming messages.
    private val consumerChannelPool = ChannelPool(connectionFactory.newConnection())

    // Channel pool for publishing messages.
    private val publisherChannelPool = ChannelPool(connectionFactory.newConnection())

    // Storage of most recent events sent by other brokers and set by this broker.
    private val latestTopicEvents = LatestTopicEvents()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumerChannelPool.isClosed || consumerChannelPool.isClosed))
    }

    /**
     * Process the message received. Send the message to the subscribers.
     * @param event The event to process.
     * @param toResend Indicates if the message should be resent.
     */
    private fun processMessage(event: Event, toResend: Boolean = false) {
        latestTopicEvents.setLatestReceivedEvent(event.topic, event)
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber ->
                if (subscriber.lastEventId < event.id) {
                    associatedSubscribers.updateLastEventIdListened(subscriber.id, event.topic, event.id)
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
            (latestEvent.message != event.message)

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
            val event = BrokerSerializer.deserializeEventFromJson(String(body))
            if (event.topic == adminTopic) {
                val latestEvent = latestTopicEvents.getLatestReceivedEvent(event.message)
                logger.info(
                    "Request Latest Event received to routinkg key {}  in the consumertag {} and exchange {}",
                    envelope.routingKey,
                    consumerTag,
                    envelope.exchange
                )
                if (latestEvent != null) {
                    channel.basicPublish(
                        broadCastExchange,
                        latestEvent.topic,
                        null,
                        BrokerSerializer.serializeEventToJson(latestEvent).toByteArray()
                    )
                }
                channel.basicAck(deliveryTag, false)
                return
            }

            logger.info(
                "event received message {} with id {} in broker {} ",
                event.message,
                event.id,
                brokerNumber
            )
            val latestEvent = latestTopicEvents.getLatestReceivedEvent(event.topic)
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
     * Create an admin event.
     */
    private fun createAdminEvent(topic: String) = Event(adminTopic, -1, topic, false)

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The callback to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerException.BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{} in broker {}", topic, subscriber.id, brokerNumber)
        val handler = getLastEvent(topic)
        if (handler != null) {
            subscriber.handler(handler)
        } else {
            requestLatestEvent(topic)
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
    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown.get()) throw BrokerException.BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        logger.info("publishing message to topic '{} with message {} in broker {}'", topic, message, brokerNumber)
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     * @throws BrokerTurnOffException If the broker is turned off.
     */
    override fun shutdown() {
        logger.info("Shutting down broker")
        if (isShutdown.compareAndSet(false, true)) {
            consumerChannelPool.close()
            publisherChannelPool.close()
        } else {
            throw BrokerException.BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    /**
     * Request the latest event from the topic.
     *
     * @param topic The topic name.
     */
    private fun requestLatestEvent(topic: String) {
        retryExecutor.execute({ BrokerException.BrokerLostConnectionException() }, {
            val requestEvent = createAdminEvent(topic)
            val responseChannel = consumerChannelPool.getChannel()
            responseChannel.basicPublish(broadCastExchange, "", null, BrokerSerializer.serializeEventToJson(requestEvent).toByteArray())
            consumerChannelPool.stopUsingChannel(responseChannel)
        }, retryCondition)
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
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * Declares the exchange and queue and binds the queue to the exchange. Starts consuming the queue.
     */
    private fun listen() {
        retryExecutor.execute({ BrokerException.BrokerLostConnectionException() }, {
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
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute(exception = { BrokerException.BrokerLostConnectionException() }, action = {
            val channel = publisherChannelPool.getChannel()
            channel.confirmSelect()
            val id = latestTopicEvents.getNextEventId(topic)
            val event = Event(topic, id, message, isLastMessage)
            latestTopicEvents.setLatestSentEvent(topic, event)
            channel.basicPublish(broadCastExchange, topic, null, BrokerSerializer.serializeEventToJson(event).toByteArray())
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
        retryExecutor.execute({ BrokerException.BrokerLostConnectionException() }, {
            val channel = publisherChannelPool.getChannel()
            latestTopicEvents.setLatestSentEvent(event.topic, event)
            channel.basicPublish(broadCastExchange, event.topic, null, BrokerSerializer.serializeEventToJson(event).toByteArray())
            publisherChannelPool.stopUsingChannel(channel)
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitFanoutExchange::class.java)

        // Connection factory used to make connections to message broker.
        private val connectionFactory = ConnectionFactory().apply {
            username = "user"
            password = "password"
        }
    }
}
