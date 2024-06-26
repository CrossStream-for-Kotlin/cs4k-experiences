package cs4k.prototype.broker.option2.redis.experiences

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment.getRedisHost
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.common.Utils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.Connection
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisShardedPubSub
import java.util.UUID
import kotlin.concurrent.thread

// [NOTE] Discontinued, mainly, because:
//    - All 'BrokerRedisJedisClusterPubSub' active instances receive all events,
//      regardless of whether they have subscribers for those events.

// - Jedis Java client
// - Redis Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)
// - Support only for Redis cluster

// @Component
class BrokerRedisJedisClusterPubSub(
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
) : Broker {

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Channel to subscribe.
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

            val event = BrokerSerializer.deserializeEventFromJson(message)
            associatedSubscribers
                .getAll(event.topic)
                .forEach { subscriber -> subscriber.handler(event) }
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
        unsubscribeChannel()
        clusterConnectionPool.close()
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
            connection.spublish(channel, BrokerSerializer.serializeEventToJson(event))
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
     */
    private fun getEventIdAndUpdateHistory(conn: JedisCluster, topic: String, message: String, isLast: Boolean): Long =
        conn.eval(
            GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT,
            listOf(topic),
            listOf(
                Event.Prop.ID.key,
                Event.Prop.MESSAGE.key,
                message,
                Event.Prop.IS_LAST.key,
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
            val map = clusterConnectionPool.hgetAll(topic)
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
        private val logger = LoggerFactory.getLogger(BrokerRedisJedisClusterPubSub::class.java)

        // Script to atomically update history and get the identifier for the event.
        private val GET_EVENT_ID_AND_UPDATE_HISTORY_SCRIPT = """
            redis.call('hsetnx', KEYS[1], ARGV[1], '-1')
            local id = redis.call('hincrby', KEYS[1], ARGV[1], 1)
            redis.call('hmset', KEYS[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
            return id
        """.trimIndent()

        /**
         * Create a cluster connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @return The cluster connection poll represented by a JedisPool instance.
         */
        private fun createClusterConnectionPool(dbConnectionPoolSize: Int): JedisCluster {
            val poolConfig: GenericObjectPoolConfig<Connection> = GenericObjectPoolConfig<Connection>()
            poolConfig.maxTotal = dbConnectionPoolSize
            val nodes = List(6) {
                HostAndPort(getRedisHost(), 7000 + it)
            }
            return JedisCluster(nodes.toSet(), poolConfig)
        }
    }
}
