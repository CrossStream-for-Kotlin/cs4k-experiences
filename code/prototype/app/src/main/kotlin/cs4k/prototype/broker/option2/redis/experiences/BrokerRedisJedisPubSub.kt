package cs4k.prototype.broker.option2.redis.experiences

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment.getRedisHost
import cs4k.prototype.broker.common.Environment.getRedisPort
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.common.Utils
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.exceptions.JedisException
import java.util.UUID
import kotlin.concurrent.thread

// [NOTE] Discontinued, mainly, because:
//    - All 'BrokerRedisJedisPubSub' active instances receive all events,
//      regardless of whether they have subscribers for those events.

// - Jedis Java client
// - Redis Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)
// - Support only for single Redis node

// @Component
class BrokerRedisJedisPubSub(
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
) : Broker {

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Channel prefix and pattern.
    private val channelPrefix = "cs4k-"
    private val channelPattern = "$channelPrefix*"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is JedisException && connectionPool.isClosed)
    }

    init {
        // Start a new thread to ...
        thread {
            // ... subscribe pattern and process events.
            subscribePattern()
        }
    }

    private val singletonJedisPubSub = object : JedisPubSub() {

        override fun onPMessage(pattern: String?, channel: String?, message: String?) {
            if (pattern == null || channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new message '{}' channel '{}'", message, channel)

            val subscribers = associatedSubscribers.getAll(channel.substringAfter(channelPrefix))
            if (subscribers.isNotEmpty()) {
                val event = BrokerSerializer.deserializeEventFromJson(message)
                subscribers.forEach { subscriber -> subscriber.handler(event) }
            }
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        publishMessage(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        isShutdown = true
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
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Subscribe pattern.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribePattern() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.resource.use {
                logger.info("psubscribe channel '{}'", channelPattern)
                it.psubscribe(singletonJedisPubSub, channelPattern)
            }
        }, retryCondition)
    }

    /**
     * UnSubscribe pattern.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribePattern() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("punsubscribe channel '{}'", channelPattern)
            singletonJedisPubSub.punsubscribe(channelPattern)
        }, retryCondition)
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.resource.use { conn ->
                val event = Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )
                conn.publish(channelPrefix + topic, BrokerSerializer.serializeEventToJson(event))
                logger.info("publish topic '{}' event '{}", topic, event)
            }
        }, retryCondition)
    }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     *
     * @param conn The connection to be used to interact with the database.
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun getEventIdAndUpdateHistory(conn: Jedis, topic: String, message: String, isLast: Boolean): Long {
        val transaction = conn.multi()
        return try {
            transaction.hsetnx(topic, Event.Prop.ID.key, "-1")
            transaction.hincrBy(topic, Event.Prop.ID.key, 1)
            transaction.hmset(
                topic,
                hashMapOf(Event.Prop.MESSAGE.key to message, Event.Prop.IS_LAST.key to isLast.toString())
            )
            transaction.exec()
                ?.getOrNull(1)
                ?.toString()
                ?.toLong()
                ?: throw UnexpectedBrokerException()
        } catch (e: Exception) {
            transaction.discard()
            throw e
        }
    }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val map = connectionPool.resource.use { conn -> conn.hgetAll(topic) }
            val id = map[Event.Prop.ID.key]?.toLong()
            val message = map[Event.Prop.MESSAGE.key]
            val isLast = map[Event.Prop.IS_LAST.key]?.toBoolean()
            return@execute if (id != null && message != null && isLast != null) {
                Event(topic, id, message, isLast)
            } else {
                null
            }
        }, retryCondition)

    private companion object {

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisJedisPubSub::class.java)

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @return The connection poll represented by a JedisPool instance.
         */
        private fun createConnectionPool(dbConnectionPoolSize: Int): JedisPool {
            val jedisPoolConfig = JedisPoolConfig()
            jedisPoolConfig.maxTotal = dbConnectionPoolSize
            return JedisPool(jedisPoolConfig, getRedisHost(), getRedisPort())
        }
    }
}
