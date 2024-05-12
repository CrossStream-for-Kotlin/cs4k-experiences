package cs4k.prototype.broker.option2.rabbitmq

// import org.springframework.stereotype.Component
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean


// [NOTE] Discontinued, mainly, because:
//      - Cannot garantee that all nodes will have the same id for the same event.
//      - Cenario of multiple publishers and multiple brokers the events can be received 2 or more times.

// - RabbitMQ Java client
// - RabbitMQ Queues (1 exchange - n queues)
// - Exchange of type direct and queues of type quorum
// - Support for RabbitMQ Cluster

// @Component
class BrokerRabbitDirectExchange : Broker {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Admin event to request the latest event.
    private val adminTopic = "Request Latest Event"

    // Exchange name for broadcasting messages.
    private val broadCastExchange = "cs4k-notifications"

    // Indetify Broker instace
    val brokerNumber = UUID.randomUUID().toString()

    // Consumer tag identifying the broker as consumer.
    private val queueTag = "cs4k-broker:" + UUID.randomUUID().toString()


    // Channel pool for consuming messages.
    private val consumerChannelPool = ChannelPool(connectionFactory.newConnection())

    //channel pool for publishing messages.
    private val publisherChannelPool = ChannelPool(connectionFactory.newConnection())

    // Storage of most recent events sent by other brokers and set by this broker.
    // private val latestTopicEvents = LatestTopicEvents()
    val latestTopicEvents = LatestTopicEvents()

    // Retry condition for retrying operations.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumerChannelPool.isClosed || publisherChannelPool.isClosed))
    }

    /**
     * Registers the event as the latest received to the topic  and sends it to all subscribers of the topic.
     * @param event The event to be processed.
     */
    private fun processMessage(event: Event) {
        latestTopicEvents.setLatestReceivedEvent(event.topic, event)
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber ->
                if (subscriber.lastEventId < event.id) {
                    associatedSubscribers.updateLastEventIdListened(subscriber.id, event.topic, event.id)
                    subscriber.handler(event)
                }
            }
    }

    /**
     * Checks if there is an id conflict between two different events.
     * @param latestEvent The latest event received.
     * @param event The event to be checked.
     */
    private fun thereIsIdConflict(latestEvent: Event, event: Event) =
        latestEvent.id == event.id &&
                (latestEvent.message != event.message)

    /**
     * Consumer handler for consuming messages.
     * @param channel The channel to consume messages.
     * @throws IOException If there is an error consuming messages.
     */
    private inner class ConsumerHandler(channel: Channel) : DefaultConsumer(channel) {

        /**
         * Handles the delivery of a message.
         * @param consumerTag The consumer tag.
         * @param envelope The envelope of the message.
         * @param properties The properties of the message.
         * @param body The body of the message.
         */
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
                "Received {}  in broker {} ",
                event,
                brokerNumber
            )
            val latestEvent = latestTopicEvents.getLatestReceivedEvent(event.topic)
            when {

                latestEvent == null || latestEvent.id < event.id -> {
                    logger.info("new event received {}", event)
                    processMessage(event)
                }

                thereIsIdConflict(latestEvent, event) -> {
                    val recentEvent = event.copy(id = event.id + 1)
                    logger.info("same event received with different id, latest updated {}", recentEvent)
                    processMessage(recentEvent)
                }

                latestEvent.id > event.id -> {
                    val recenteEvent = event.copy(id = latestEvent.id + 1)
                    processMessage(recenteEvent)
                    logger.info("older event received, thrown away")
                }
            }
            channel.basicAck(deliveryTag, false)
        }
    }

    init {
        listen()
    }

    // Indicates if the broker is shutdown.
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
    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (topic == adminTopic) throw IllegalArgumentException("Cannot subscribe to admin topic.")
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        bindTopicToQueue(topic)
        logger.info("new subscriber topic '{}' in broker {}", topic, brokerNumber)
        val latestEvent = getLastEvent(topic)
        if (latestEvent != null) {
            subscriber.handler(latestEvent)
        } else {
            requestLatestEvent(topic)
        }

        return { unsubscribe(topic, subscriber) }
    }

    /**
     * Bind a topic to a queue.
     * @param topic The topic name.
     */
    private fun bindTopicToQueue(topic: String) {
        val channel = consumerChannelPool.getChannel()
        channel.queueBind(queueTag, broadCastExchange, topic)
        consumerChannelPool.stopUsingChannel(channel)
    }

    /**
     * Get the last event received in a topic.
     */
    private fun getLastEvent(topic: String): Event? {
        val event = latestTopicEvents.getLatestEvent(topic)
        logger.info("last event received -> {}, Received in broker {} ", event, brokerNumber)
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
        if (topic == adminTopic) throw IllegalArgumentException("Cannot publish to admin topic.")
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        logger.info("publishing message to topic '{} with message {} in broker {}'", topic, message, brokerNumber)
        bindTopicToQueue(topic)
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     * Closes all channels and connections.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    override fun shutdown() {
        logger.info("Shutting down broker")
        if (isShutdown.compareAndSet(false, true)) {
            consumerChannelPool.close()
            publisherChannelPool.close()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
    }

    /**
     * Request the latest event from a topic.
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun requestLatestEvent(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val requestEvent = createAdminEvent(topic)
            val responseChannel = consumerChannelPool.getChannel()
            responseChannel.basicPublish(
                broadCastExchange,
                topic,
                null,
                BrokerSerializer.serializeEventToJson(requestEvent).toByteArray()
            )
            consumerChannelPool.stopUsingChannel(responseChannel)
        }, retryCondition)
    }

    /**
     * Create an admin event.
     */
    private fun createAdminEvent(topic: String) = Event(adminTopic, -1, topic, false)

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
     * Listen for notifications and creates the exchange, queue and binds the queue to the exchange. Also consumes the queue.
     * Declares a exchange.
     * Declares a queue and binds it to the exchange.
     * Consumes the queue.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun listen() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val channel = consumerChannelPool.getChannel()
            channel.exchangeDeclare(broadCastExchange, "direct")
            channel.queueDeclare(
                queueTag,
                true,
                false,
                false,
                mapOf("x-max-length" to 100_000, "x-queue-type" to "quorum")
            )
            channel.queueBind(queueTag, broadCastExchange, "")
            channel.queueBind(queueTag, broadCastExchange, adminTopic)
            channel.basicConsume(queueTag, false, queueTag, ConsumerHandler(channel))
            consumerChannelPool.registerConsuming(channel, queueTag)
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
        retryExecutor.execute(exception = { BrokerLostConnectionException() }, action = {
                val channel = publisherChannelPool.getChannel()
                val id = latestTopicEvents.getNextEventId(topic)
                val event = Event(topic, id, message, isLastMessage)
                bindTopicToQueue(topic)
                channel.basicPublish(
                    broadCastExchange,
                    topic,
                    null,
                    BrokerSerializer.serializeEventToJson(event).toByteArray()
                )
                logger.info("Message successfully published to topic: $topic with message: $message")
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
            try {
                val channel = publisherChannelPool.getChannel()
                bindTopicToQueue(event.topic)
                channel.basicPublish(
                    broadCastExchange,
                    event.topic,
                    null,
                    BrokerSerializer.serializeEventToJson(event).toByteArray()
                )
                publisherChannelPool.stopUsingChannel(channel)
            } catch (e: Exception) {
                logger.error("Error publishing message", e)
            }
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitDirectExchange::class.java)

        // Connection factory used to make connections to message broker.
        private val connectionFactory = ConnectionFactory().apply {
            username = "user"
            password = "password"
        }
    }
}
