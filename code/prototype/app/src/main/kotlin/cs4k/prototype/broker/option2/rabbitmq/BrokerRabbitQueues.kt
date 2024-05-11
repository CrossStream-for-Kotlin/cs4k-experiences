package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.BrokerException.*
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
//import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

// @Component
class BrokerRabbitQueues : Broker {

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Admin event para fazer o pedido do ultimo evento
    private val adminEvent = "Request Latest Event"

    // Name of exchange used to publish messages to.
    private val broadCastExchange = "cs4k-notifications"

    //Indetify Broker instace
    val brokerNumber = UUID.randomUUID().toString()

    // Consumer tag identifying the broker as consumer.
    private val queueTag = "cs4k-broker:" + UUID.randomUUID().toString()


    // Channel pool
    private val consumerChannelPool = ChannelPool(connectionFactory.newConnection())
    private val publisherChannelPool = ChannelPool(connectionFactory.newConnection())



    // Storage of most recent events sent by other brokers and set by this broker.
    //private val latestTopicEvents = LatestTopicEvents()
    val latestTopicEvents = LatestTopicEvents()


    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is IOException && (consumerChannelPool.isClosed || publisherChannelPool.isClosed))
    }

    /**
     * Registers the event as the latest received and sends it to all subscribers.
     */
    private fun processMessage(event: Event, toResend: Boolean = false) {
        latestTopicEvents.setLatestReceivedEvent(event.topic, event)
        latestTopicEvents.setEventToTopic(event.topic, event)
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber ->
                if (subscriber.lastEventId < event.id) {
                    associatedSubscribers.updateLastEventIdListened(subscriber.id, event.topic, event.id)
                    subscriber.handler(event)
                }
            }
        /*if (toResend) {
            notify(event)
        }*/
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
            val latestEvent = latestTopicEvents.getLatestReceivedEvent(event.topic)
            if (event.message == adminEvent) {
                if (latestEvent != null) {
                    channel.basicPublish(
                        broadCastExchange, latestEvent.topic,
                        null,
                        BrokerSerializer.serializeEventToJson(latestEvent).toByteArray()
                    )
                }
                channel.basicAck(deliveryTag, false)
                return
            }
            val notConsumedYet =
                latestTopicEvents.getAllReceivedEvents(event.topic)
                    .firstOrNull { it.timestamp == event.timestamp && it.message == event.message }
            logger.info(
                "Received {}  in broker {} ",
                event,
                brokerNumber
            )
            when {
                latestEvent == null || latestEvent.id < event.id -> {
                    logger.info("new event received {}", event)
                    processMessage(event)
                }

                latestEvent != null && thereIsIdConflict(latestEvent, event) -> {
                    val recentEvent = event.copy(id = event.id + 1)
                    logger.info("same event received with different id, latest updated {}", recentEvent)
                    processMessage(recentEvent)
                }

                latestEvent.id > event.id -> {
                    val recenteEvent = event.copy(id = latestEvent.id + 1)
                    processMessage(recenteEvent)
                    logger.info("older event received, thrown away")
                }


                /*latestEvent != null && thereIsIdConflict(latestEvent, event) -> {
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

                latestEvent.id < event.id -> {
                    if(notConsumedYet == null){
                        val recenteEvent = event.copy(id = latestEvent.id + 1)
                        processMessage(recenteEvent, true)
                        logger.info("older event received but not yet consumed")
                    }
                    logger.info("older event received, thrown away")
                }*/
            }

            /*
                            notConsumedYet != null -> {
                                logger.info("Ignored event already consumed {} in broker {} ", notConsumedYet, brokerNumber)
                            }

                            latestEvent != null && thereIsIdConflict(latestEvent, event) && notConsumedYet == null -> {
                                val recentEvent = event.copy(id = event.id + 1)
                                logger.info(
                                    "same event received with different id, updated latest {} in broker {}",
                                    recentEvent,
                                    brokerNumber
                                )
                                processMessage(recentEvent, true)
                            }

                            (latestEvent == null || latestEvent.id < event.id) && notConsumedYet == null -> {
                                logger.info(
                                    "new event received {} latest event {} in broker {}",
                                    event,
                                    latestEvent,
                                    brokerNumber
                                )
                                processMessage(event)
                            }

                            latestEvent == event && notConsumedYet == null && latestEvent != null -> {
                                // countNumberTimesReceived.remove([Pair(event.topic, event.message)])
                                logger.info(
                                    "same event received, thrown away , latest message {} id {} event message {} id {}   ",
                                    latestEvent.message,
                                    latestEvent.id,
                                    event.message,
                                    event.id
                                )
                            }

                            latestEvent != null && latestEvent.id > event.id && notConsumedYet == null -> {
                                val recenteEvent = event.copy(id = latestEvent.id + 1)
                                logger.info("Ainda nao foi consumido {} in broker {}", recenteEvent, brokerNumber)
                                processMessage(recenteEvent, false)
                                logger.info("older event received {}, thrown away with recent event {}  ", latestEvent, event)
                            }
                        }*/
            channel.basicAck(deliveryTag, false)
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
    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown.get()) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        bindTopicToQueue(topic)
        logger.info("new subscriber topic '{}' in broker {}", topic, brokerNumber)
        val handler = getLastEvent(topic)
        if (handler != null) {
            subscriber.handler(handler)
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

    private fun requestLatestEvent(topic: String) {
        try {
            val requestEvent = createAdminEvent(topic)
            val responseChannel = consumerChannelPool.getChannel()
            responseChannel.basicPublish(broadCastExchange, topic, null, BrokerSerializer.serializeEventToJson(requestEvent).toByteArray())
            consumerChannelPool.stopUsingChannel(responseChannel)
        } catch (e: Exception) {
            logger.error("Error requesting latest event", e)
        }
    }

    /**
     * Create an admin event to request the latest event.
     */
    private fun createAdminEvent(topic: String) = Event(topic, 0, adminEvent, false)

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
            channel.basicConsume(queueTag, false, queueTag, ConsumerHandler(channel))
            consumerChannelPool.registerConsuming(channel, queueTag)
        }, retryCondition)
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen() {
        consumerChannelPool.close()
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
            try {
                val channel = publisherChannelPool.getChannel()
                val id = latestTopicEvents.getNextEventId(topic)
                val event = Event(topic, id, message, isLastMessage)
                //latestTopicEvents.setLatestSentEvent(topic, event)
                bindTopicToQueue(topic)
                channel.basicPublish(broadCastExchange, topic, null, BrokerSerializer.serializeEventToJson(event).toByteArray())
                logger.info("Message successfully published to topic: $topic with message: $message")
                publisherChannelPool.stopUsingChannel(channel)
            } catch (e: Exception) {
                logger.error("Error publishing message", e)
            }
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
                //latestTopicEvents.setLatestSentEvent(event.topic, event)
                bindTopicToQueue(event.topic)
                channel.basicPublish(broadCastExchange, event.topic, null, BrokerSerializer.serializeEventToJson(event).toByteArray())
                publisherChannelPool.stopUsingChannel(channel)
            } catch (e: Exception) {
                logger.error("Error publishing message", e)
            }
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRabbitQueues::class.java)

        // Connection factory used to make connections to message broker.
        private val connectionFactory = ConnectionFactory().apply {
            username = "user"
            password = "password"
        }
    }
}

