package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
//import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

// @Component
class BrokerRabbitQueues {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Name of exchange used to publish messages to.
    private val broadCastExchange = "cs4k-notifications"

    // Consumer tag identifying the broker as consumer.
    private val consumerTag = "cs4k-broker:" + UUID.randomUUID().toString()


    // Connection and channel used for notifications.
    private val acceptingConnection = connectionFactory.newConnection()

    //Channel pool and channel used
    private val channelPool = ChannelPool(acceptingConnection)
    private val acceptingChannel = channelPool.getChannel()

    // Concurrent queue to store the channels that are being used.
    private val usingChannels = ConcurrentLinkedQueue<Channel>()

    // Storage of most recent events sent by other brokers and set by this broker.
    private val latestTopicEvents = LatestTopicEvents()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && !acceptingChannel.isOpen)
    }



    private val consumer = object : DefaultConsumer(acceptingChannel) {

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
                    (latestEvent.message != event.message && latestEvent.isLast != event.isLast)

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
            when {
                latestEvent == null || latestEvent.id < event.id -> {
                    logger.info("new event received {}", event)
                    processMessage(event)
                }

                thereIsIdConflict(latestEvent, event) -> {
                    val recentEvent = event.copy(id = event.id + 1)
                    logger.info("same event received with different id, latest updated {}", recentEvent)
                    processMessage(recentEvent, true)
                }

                latestEvent == event -> {
                    logger.info("same event received, thrown away")
                }

                latestEvent.id > event.id -> {
                    logger.info("older event received, thrown away")
                }
            }
            acceptingChannel.basicAck(deliveryTag, false)
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
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

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
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            unListen()
            acceptingChannel.close()
            acceptingConnection.close()
        } else {
            throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")
        }
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
        acceptingChannel.exchangeDeclare(broadCastExchange, "fanout", true)
        acceptingChannel.queueDeclare(
            consumerTag,
            true,
            false,
            false,
            mapOf("x-max-length" to 100_000, "x-queue-type" to "quorum")
        )
        acceptingChannel.queueBind(consumerTag, broadCastExchange, "")
        acceptingChannel.basicConsume(consumerTag, false, consumerTag, consumer)
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
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            val channel = channelPool.getChannel()
            val id = latestTopicEvents.getNextEventId(topic)
            val event = Event(topic, id, message, isLastMessage)
            latestTopicEvents.setLatestSentEvent(topic, event)
            channel.basicPublish(broadCastExchange, topic, null, serialize(event).toByteArray())
            usingChannels.add(channel)
        }, retryCondition)
    }

    /**
     * Notify the topic with the event.
     *
     * @param event The event to be sent.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun notify(event: Event) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            val channel = channelPool.getChannel()
            latestTopicEvents.setLatestSentEvent(event.topic, event)
            channel.basicPublish(broadCastExchange, event.topic, null, serialize(event).toByteArray())
            usingChannels.add(channel)
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitQueues::class.java)

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



/**
//generate all consumers tags for the broker
//private val consumerTags: MutableList<String> = mutableListOf("$consumerTag-1","$consumerTag-2","$consumerTag-3")
/*
    /**
     * Listen for notifications with multiple consumer threads.
     */
    private fun listenWithMultipleConsumers(consumerTags: List<String> ,consumerCount: Int = 3) {

        /**
         * Registers the event as the latest received and sends it to all subscribers.
         */
        fun processMessage2(event: Event) {
            latestTopicEvents.setLatestReceivedEvent(event.topic, event)
            associatedSubscribers
                .getAll(event.topic)
                .forEach { subscriber ->
                    if (subscriber.lastEventNotified < event.id) {
                        associatedSubscribers.updateLastEventListened(subscriber.id, event.topic, event.id)
                        subscriber.handler(event)
                    }
                }
            notify(event)
        }

        /**
         * Checks if there is an id conflict between two different events.
         */
        fun thereIsIdConflict(latestEvent: Event, event: Event) =
            latestEvent.id == event.id &&
                    (latestEvent.message != event.message && latestEvent.isLast != event.isLast)


        for (i in 1..consumerCount) {
            val channel = acceptingChannel
            val consumerTag = consumerTags[i-1]
            channel.queueDeclare(
                consumerTag,
                true,
                false,
                false,
                mapOf("x-max-length" to 100_000, "x-queue-type" to "quorum")
            )

            channel.queueBind(consumerTag, broadCastExchange, "")
            val consumer = object : DefaultConsumer(channel) {
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
                    when {
                        latestEvent == null || latestEvent.id < event.id -> {
                            logger.info("new event received {}", event)
                            processMessage2(event)
                        }

                        thereIsIdConflict(latestEvent, event) -> {
                            val recentEvent = event.copy(id = event.id + 1)
                            logger.info("same event received with different id, latest updated {}", recentEvent)
                            processMessage2(recentEvent)
                        }

                        latestEvent == event -> {
                            logger.info("InstanceId=${brokerNumber} same event received, thrown away")
                        }

                        latestEvent.id > event.id -> {
                            logger.info("older event received, thrown away")
                        }
                    }
                    channel.basicAck(deliveryTag, false)
                }
            }
            channel.basicConsume(consumerTag, false, consumerTag, consumer)
        }
    }
        **/