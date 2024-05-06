package cs4k.prototype.broker.option2.redis.experiences

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.ConnectionPoolSizeException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.Environment.getRedisHost
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.option2.redis.EventProp
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisShardedPubSub
import java.util.UUID
import kotlin.concurrent.thread

// - Jedis client
// - Redis Cluster Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)

// Not in use because:
//    - All 'BrokerRedisJedisClusterPubSub' instances receive all events,
//      regardless of whether they have subscribers for those events.

// @Component
class BrokerRedisJedisClusterPubSub(
    private val dbConnectionPoolSize: Int = 10
) {

    // Broker shutdown state.
    private var isShutdown = false

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Channel.
    private val channel = "share_channel"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Cluster Connection pool.
    private val clusterConnectionPool = retryExecutor.execute({ BrokerConnectionException() }, {
        createClusterConnectionPool(dbConnectionPoolSize)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { _ -> true }

    init {
        // Start a new thread to subscribe a common channel and process events.
        thread {
            subscribeChannel()
        }
    }

    private val singletonJedisPubSub = object : JedisShardedPubSub() {

        override fun onSMessage(channel: String?, message: String?) {
            if (channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new message '{}' channel '{}'", message, channel)

            val event = deserialize(message)
            associatedSubscribers
                .getAll(event.topic)
                .forEach { subscriber -> subscriber.handler(event) }
        }
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
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        unsubscribeChannel()
        clusterConnectionPool.close()
        isShutdown = true
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
     * Subscribe a common channel.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribeChannel() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("ssubscribe channel '{}'", channel)
            clusterConnectionPool.ssubscribe(singletonJedisPubSub, channel)
        }, retryCondition)
    }

    /**
     * UnSubscribe a common channel.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribeChannel() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            logger.info("sunsubscribe channel '{}'", channel)
            singletonJedisPubSub.sunsubscribe(channel)
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
            val connection = clusterConnectionPool
            val event = Event(
                topic = topic,
                id = getEventIdAndUpdateHistory(connection, topic, message, isLastMessage),
                message = message,
                isLast = isLastMessage
            )
            connection.spublish(channel, serialize(event))
            logger.info("spublish topic '{}' event '{}", topic, event)
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
    private fun getEventIdAndUpdateHistory(conn: JedisCluster, topic: String, message: String, isLast: Boolean): Long =
        conn.eval(
            GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT,
            listOf(topic),
            listOf(
                EventProp.ID.toString(),
                EventProp.MESSAGE.toString(),
                message,
                EventProp.IS_LAST.toString(),
                isLast.toString()
            )
        ).toString().toLong()

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            val lastEventProps = clusterConnectionPool.hgetAll(topic)
            val id = lastEventProps["${EventProp.ID}"]?.toLong()
            val message = lastEventProps["${EventProp.MESSAGE}"]
            val isLast = lastEventProps["${EventProp.IS_LAST}"]?.toBoolean()
            return@execute if (id != null && message != null && isLast != null) {
                Event(topic, id, message, isLast)
            } else {
                null
            }
        }, retryCondition)

    private companion object {

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisJedisClusterPubSub::class.java)

        // Minimum database connection pool size allowed.
        const val MIN_DB_CONNECTION_POOL_SIZE = 2

        // Maximum database connection pool size allowed.
        const val MAX_DB_CONNECTION_POOL_SIZE = 100

        // Number of redis nodes in cluster.
        // --- Change to Environment Variable ----
        private const val NUMBER_OF_REDIS_NODES = 6

        // Port number of first redis node in cluster.
        // --- Change to Environment Variable ----
        private const val START_REDIS_PORT = 7000

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
         * Create a cluster connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The optional maximum size that the pool is allowed to reach.
         * @return The cluster connection poll represented by a JedisPool instance.
         */
        private fun createClusterConnectionPool(dbConnectionPoolSize: Int = 10): JedisCluster {
            val poolConfig: GenericObjectPoolConfig<Connection> = GenericObjectPoolConfig<Connection>()
            poolConfig.maxTotal = dbConnectionPoolSize
            val nodes = List(NUMBER_OF_REDIS_NODES) {
                HostAndPort(getRedisHost(), START_REDIS_PORT + it)
            }
            return JedisCluster(nodes.toSet(), poolConfig)
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
