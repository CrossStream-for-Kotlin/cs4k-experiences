package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.BrokerException.BrokerDbConnectionException
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerOptimisticLockingException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.BrokerException.DbConnectionPoolSizeException
import cs4k.prototype.broker.BrokerException.UnexpectedBrokerException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.exceptions.JedisException
import java.util.UUID
import kotlin.concurrent.thread

// V1:
//      - Redis Pub/Sub using Redis as an in-memory data structure (key-value pair)
//      - Jedis client

@Component
class BrokerRedis(
    private val dbConnectionPoolSize: Int = 10
) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Topic prefix and pattern to subscribe.
    private val topicPrefix = "cs4k:"
    private val topicPattern = "$topicPrefix*"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is JedisException && connectionPool.isClosed)
    }

    init {
        // Start a new thread to subscribe and process events.
        thread {
            subscribePattern()
        }
    }

    private val singletonPubSub = object : JedisPubSub() {
        override fun onPSubscribe(pattern: String?, subscribedChannels: Int) {
            logger.info("onPSubscribe called, pattern '{}' subscribedChannels '{}", pattern, subscribedChannels)
            super.onPSubscribe(pattern, subscribedChannels)
        }

        override fun onPMessage(pattern: String?, channel: String?, message: String?) {
            if (pattern == null || channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new event '{}' pattern '{}' channel '{}'", message, pattern, channel)
            associatedSubscribers.getAll(channel.substringAfter(topicPrefix)).let { subscribers ->
                val event = deserialize(message)
                subscribers.forEach { subscriber -> subscriber.handler(event) }
            }
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
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

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
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        internalPublish(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")
        unsubscribePattern()
        connectionPool.close()
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
     * Subscribe pattern.
     *
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribePattern() {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.resource.use {
                logger.info("psubscribe channel '{}'", topicPattern)
                it.psubscribe(singletonPubSub, topicPattern)
            }
        }, retryCondition)
    }

    /**
     * UnSubscribe pattern.
     *
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribePattern() {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            logger.info("punsubscribe channel '{}'", topicPattern)
            singletonPubSub.punsubscribe(topicPattern)
        }, retryCondition)
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun internalPublish(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.resource.use { jedis ->
                val event = Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(jedis, topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )
                jedis.publish(topicPrefix + topic, serialize(event))
                logger.info("notify topic '{}' event '{}", topic, event)
            }
        }, retryCondition)
    }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     *
     * @param jedis The connection to be used to interact with the database.
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun getEventIdAndUpdateHistory(jedis: Jedis, topic: String, message: String, isLast: Boolean): Long {
        jedis.watch(topic)
        val transaction = jedis.multi()
        return try {
            transaction
                .apply {
                    hsetnx(topic, "id", "-1")
                    hincrBy(topic, "id", 1)
                    hset(topic, "message", message)
                    hset(topic, "isLast", isLast.toString())
                }
                .exec()
                ?.getOrNull(1)
                ?.toString()
                ?.toLong()
                ?: throw BrokerOptimisticLockingException()
        } catch (e: JedisException) {
            transaction.discard()
            throw e
        } finally {
            jedis.unwatch()
        }

        /* val lastEvent = jedis.get(topic)
        jedis.watch(topic)
        val transaction = jedis.multi()
        val eventId = if (lastEvent == null) 0 else deserialize(lastEvent).id + 1
        transaction.set(topic, serialize(Event(topic, eventId, message, isLast)))
        transaction.exec()
        return eventId */
    }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.resource.use { jedis ->
                // jedis.watch(topic)
                val lastEventProps = jedis.hgetAll(topic)
                val id = lastEventProps["id"]?.toLong()
                val message = lastEventProps["message"]
                val isLast = lastEventProps["isLast"]?.toBoolean()
                return@execute if (id != null && message != null && isLast != null) {
                    Event(topic, id, message, isLast)
                } else {
                    null
                }
            }

            /* connectionPool.resource.use { jedis ->
                jedis.watch(topic)
                val transaction = jedis.multi()
                transaction.get(topic)
                val event = transaction.exec().firstOrNull()?.toString()
                if (event == null) null else deserialize(event)
            } */
        }, retryCondition)

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedis::class.java)

        // Minimum database connection pool size allowed.
        const val MIN_DB_CONNECTION_POOL_SIZE = 2

        // Maximum database connection pool size allowed.
        const val MAX_DB_CONNECTION_POOL_SIZE = 100

        /**
         * Check if the provided database connection pool size is within the acceptable range.
         *
         * @param dbConnectionPoolSize The size of the database connection pool to check.
         * @throws DbConnectionPoolSizeException If the size is outside the acceptable range.
         */
        private fun checkDbConnectionPoolSize(dbConnectionPoolSize: Int) {
            if (dbConnectionPoolSize !in MIN_DB_CONNECTION_POOL_SIZE..MAX_DB_CONNECTION_POOL_SIZE) {
                throw DbConnectionPoolSizeException(
                    "The connection pool size must be between $MIN_DB_CONNECTION_POOL_SIZE and $MAX_DB_CONNECTION_POOL_SIZE."
                )
            }
        }

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The optional maximum size that the pool is allowed to reach.
         * @return The connection poll represented by a JedisPool instance.
         */
        private fun createConnectionPool(dbConnectionPoolSize: Int = 10): JedisPool {
            val jedisPoolConfig = JedisPoolConfig()
            jedisPoolConfig.maxTotal = dbConnectionPoolSize
            return JedisPool(jedisPoolConfig, Environment.getRedisHost(), Environment.getRedisPort())
        }

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

// V2:
//      - Redis Streams
//      - Letture client

// ... In progress ...
