package cs4k.prototype.broker.option2.redis

import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.ConnectionPoolSizeException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisException
import io.lettuce.core.RedisURI
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import java.util.UUID

// - Lettuce client
// - Redis [Cluster] Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)

// @Component
class BrokerRedisLettucePubSub(
    private val dbConnectionPoolSize: Int = DEFAULT_DB_CONNECTION_POOL_SIZE
) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        createRedisClient()
        // createRedisClusterClient()
    })

    // Connection to asynchronous subscribe, unsubscribe and publish.
    private val pubSubConnection = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        redisClient.connectPubSub()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize, redisClient)
        // createClusterConnectionPool(dbConnectionPoolSize, redisClient)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        !(throwable is RedisException && (!pubSubConnection.isOpen || connectionPool.isClosed))
    }

    private val singletonRedisPubSubAdapter = object : RedisPubSubAdapter<String, String>() {

        override fun message(channel: String?, message: String?) {
            if (channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new message '{}' channel '{}'", message, channel)

            val subscribers = associatedSubscribers.getAll(channel)
            if (subscribers.isNotEmpty()) {
                val event = BrokerSerializer.deserializeEventFromJson(message)
                subscribers.forEach { subscriber -> subscriber.handler(event) }
            }
        }
    }

    init {
        // Add a listener.
        pubSubConnection.addListener(singletonRedisPubSubAdapter)
    }

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The method to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            subscribeTopic(topic)
        }
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
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        publishMessage(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     */
    fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        isShutdown = true
        pubSubConnection.removeListener(singletonRedisPubSubAdapter)
        pubSubConnection.close()
        connectionPool.close()
        redisClient.shutdown()
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(
            topic = topic,
            predicate = { sub -> sub.id == subscriber.id },
            onTopicRemove = { unsubscribeTopic(topic) }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Subscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("subscribe new topic '{}'", topic)
            pubSubConnection.async().subscribe(topic)
        }, retryCondition)
    }

    /**
     * UnSubscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("unsubscribe gone topic '{}'", topic)
            pubSubConnection.async().unsubscribe(topic)
        }, retryCondition)
    }

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val event = Event(
                topic = topic,
                id = getEventIdAndUpdateHistory(topic, message, isLastMessage),
                message = message,
                isLast = isLastMessage
            )
            pubSubConnection.async().publish(topic, BrokerSerializer.serializeEventToJson(event))
            logger.info("publish topic '{}' event '{}", topic, event)
        }, retryCondition)
    }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     *
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     */
    private fun getEventIdAndUpdateHistory(topic: String, message: String, isLast: Boolean): Long =
        connectionPool.borrowObject().use { conn ->
            conn.sync().eval(
                GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT,
                ScriptOutputType.INTEGER,
                arrayOf(topic),
                Event.Prop.ID.key,
                Event.Prop.MESSAGE.key,
                message,
                Event.Prop.IS_LAST.key,
                isLast.toString()
            )
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
            val lastEventProps = connectionPool.borrowObject().use { conn ->
                conn.sync().hgetall(topic)
            }
            val id = lastEventProps[Event.Prop.ID.key]?.toLong()
            val message = lastEventProps[Event.Prop.MESSAGE.key]
            val isLast = lastEventProps[Event.Prop.IS_LAST.key]?.toBoolean()
            return@execute if (id != null && message != null && isLast != null) {
                Event(topic, id, message, isLast)
            } else {
                null
            }
        }, retryCondition)

    private companion object {

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisLettucePubSub::class.java)

        // Default database connection pool size.
        private const val DEFAULT_DB_CONNECTION_POOL_SIZE = 10

        // Minimum database connection pool size allowed.
        private const val MIN_DB_CONNECTION_POOL_SIZE = 2

        // Maximum database connection pool size allowed.
        private const val MAX_DB_CONNECTION_POOL_SIZE = 100

        // Script to atomically update history and get the identifier for the event.
        private val GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT = """
            redis.call('hsetnx', KEYS[1], ARGV[1], '-1')
            local id = redis.call('hincrby', KEYS[1], ARGV[1], 1)
            redis.call('hmset', KEYS[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
            return id
        """.trimIndent()

        /**
         * Check if the provided database connection pool size is within the acceptable range.
         *
         * @param dbConnectionPoolSize The size of the database connection pool to check.
         * @throws ConnectionPoolSizeException If the size is outside the acceptable range.
         */
        private fun checkDbConnectionPoolSize(dbConnectionPoolSize: Int) {
            if (dbConnectionPoolSize !in MIN_DB_CONNECTION_POOL_SIZE..MAX_DB_CONNECTION_POOL_SIZE) {
                throw ConnectionPoolSizeException(
                    "The connection pool size must be between $MIN_DB_CONNECTION_POOL_SIZE and $MAX_DB_CONNECTION_POOL_SIZE."
                )
            }
        }

        /**
         * Create a redis client for database interactions.
         *
         * @return The redis client instance.
         */
        private fun createRedisClient() =
            RedisClient.create(RedisURI.create(Environment.getRedisHost(), Environment.getRedisPort()))

        /**
         * Create a redis cluster client for database interactions.
         * TODO (Use environment variables for hosts and ports of Redis nodes)
         *
         * @return The redis cluster client instance.
         */
        private fun createRedisClusterClient() =
            RedisClusterClient.create(
                List(6) {
                    RedisURI.Builder.redis("localhost", 7000 + it).build()
                }
            )

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The optional maximum size that the pool is allowed to reach.
         * @param client The redis client instance.
         * @return The connection poll represented by a GenericObjectPool instance.
         */
        private fun createConnectionPool(
            dbConnectionPoolSize: Int,
            client: RedisClient
        ): GenericObjectPool<StatefulRedisConnection<String, String>> {
            val pool = GenericObjectPoolConfig<StatefulRedisConnection<String, String>>()
            pool.maxTotal = dbConnectionPoolSize
            return ConnectionPoolSupport.createGenericObjectPool({ client.connect() }, pool)
        }

        /**
         * Create a cluster connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The optional maximum size that the pool is allowed to reach.
         * @param clusterClient The redis cluster client instance.
         * @return The cluster connection poll represented by a GenericObjectPool instance.
         */
        private fun createClusterConnectionPool(
            dbConnectionPoolSize: Int,
            clusterClient: RedisClusterClient
        ): GenericObjectPool<StatefulRedisClusterConnection<String, String>> {
            val pool = GenericObjectPoolConfig<StatefulRedisClusterConnection<String, String>>()
            pool.maxTotal = dbConnectionPoolSize
            return ConnectionPoolSupport.createGenericObjectPool({ clusterClient.connect() }, pool)
        }
    }
}
