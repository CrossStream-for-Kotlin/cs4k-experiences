package cs4k.prototype.broker.option2.redis.experiences

import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.ConnectionPoolSizeException
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.XReadArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.coroutines
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import io.lettuce.core.support.ConnectionPoolSupport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlin.coroutines.cancellation.CancellationException

// - Lettuce client
// - Redis Streams (n streams - n topics)

// Not in use because:
//      - One stream per topic means that the number of streams can scale uncontrollably.
//      - Some indeterminate behavior.

// @Component
class BrokerRedisLettuceNStreams(
    private val dbConnectionPoolSize: Int = 10
) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Stream base key.
    private val streamKey = "cs4k-*"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Listening topics.
    private val listeningTopic = ListeningTopics()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        // createRedisClient()
        createRedisClusterClient()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        // createConnectionPool(dbConnectionPoolSize, redisClient)
        createClusterConnectionPool(dbConnectionPoolSize, redisClient)
    })

    // Connection to read streams.
    private val connection = connectionPool.borrowObject()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { throwable ->
        throwable !is CancellationException
    }

    init {
        // Start a new thread ...
        thread {
            // ... to launch coroutines in Coroutine Scope.
            runBlocking {
                listenTopicsLoop(this)
            }
        }
    }

    /**
     * Check for new topics to listen.
     *
     * @param scope The Coroutine Scope to launch coroutines.
     */
    private suspend fun listenTopicsLoop(scope: CoroutineScope) {
        while (true) {
            listeningTopic.get().forEach {
                val job = scope.launch(coroutineDispatcher) {
                    listenStream(it.topic, it.startStreamMessageId)
                }
                listeningTopic.alter(it.topic, it.startStreamMessageId, job)
            }
            delay(1000)
        }
    }

    /**
     * Listen the stream associated to topic.

     * @param topic The topic name.
     * @param startStreamMessageId The identifier of the last message read from stream.
     */
    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun listenStream(topic: String, startStreamMessageId: String) {
        retryExecutor.suspendExecute({ BrokerLostConnectionException() }, {
            val coroutines = connection.coroutines()
            var currentStreamMessageId = startStreamMessageId

            while (connection.isOpen) {
                coroutines
                    .xread(
                        XReadArgs.Builder.block(BLOCK_READ_TIME),
                        XReadArgs.StreamOffset.from(streamKey.replace("*", topic), currentStreamMessageId)
                    ).collect { msg ->
                        logger.info("new message id '{}' topic '{}", msg.id, topic)
                        currentStreamMessageId = msg.id
                        processMessage(topic, msg.body)
                    }
            }
        }, retryCondition)
    }

    /**
     * Process the message, i.e., create an event from the message and call the handler of the associated subscribers.
     *
     * @param body The message properties.
     */
    private fun processMessage(topic: String, body: Map<String, String>) {
        val subscribers = associatedSubscribers.getAll(topic)
        if (subscribers.isNotEmpty()) {
            val event = createEvent(topic, body)
            subscribers.forEach { subscriber -> subscriber.handler(event) }
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

        val lastMessage = getLastStreamMessage(topic)

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            listeningTopic.add(topic, lastMessage?.id ?: "0")
        }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        lastMessage?.let { msg -> handler(createEvent(topic, msg.body)) }

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

        addMessageToStream(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        connection.close()
        connectionPool.close()
        redisClient.shutdown()
        isShutdown = true
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(
            topic = topic,
            predicate = { sub -> sub.id == subscriber.id },
            onTopicRemove = { listeningTopic.remove(topic) }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Add a message to the stream associated to topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun addMessageToStream(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.borrowObject().use { conn ->
                val sync = conn.sync()
                val newStreamEntry = mapOf(
                    Event.Prop.MESSAGE to message,
                    Event.Prop.ID to (getLastEvent(topic, sync)?.let { it.id + 1 } ?: 0).toString(),
                    Event.Prop.IS_LAST to isLastMessage.toString()
                )
                val id = sync.xadd(streamKey.replace("*", topic), newStreamEntry)
                logger.info("publish topic '{}' id '{}", topic, id)
            }
        }, retryCondition)
    }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @param sync The RedisCommands API for the current connection.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String, sync: RedisAdvancedClusterCommands<String, String>): Event? =
        getLastStreamMessage(topic, sync)?.let { msg -> createEvent(topic, msg.body) }

    /**
     * Get the latest stream message.
     *
     * @param topic The topic name.
     * @return The latest stream message, or null if the stream is empty.
     */
    private fun getLastStreamMessage(topic: String) =
        connectionPool.borrowObject().use { conn ->
            getLastStreamMessage(topic, conn.sync())
        }

    /**
     * Get the latest stream message.
     *
     * @param topic The topic name.
     * @param sync The RedisCommands API for the current connection.
     * @return The latest stream message, or null if the stream is empty.
     */
    private fun getLastStreamMessage(topic: String, sync: RedisAdvancedClusterCommands<String, String>) =
        sync.xrevrange(
            streamKey.replace("*", topic),
            Range.unbounded(),
            Limit.create(OFFSET, COUNT)
        ).firstOrNull()

    /**
     * Create an event.
     *
     * @param topic The topic of the message.
     * @param body The message properties.
     * @return The resulting event.
     */
    private fun createEvent(topic: String, body: Map<String, String>): Event {
        val id = body[Event.Prop.ID.key]?.toLong()
        val message = body[Event.Prop.MESSAGE.key]
        val isLast = body[Event.Prop.IS_LAST.key]?.toBoolean()
        if (id == null || message == null || isLast == null) throw BrokerException.UnexpectedBrokerException()
        return Event(topic, id, message, isLast)
    }

    private companion object {

        // The time that blocks reading the stream.
        const val BLOCK_READ_TIME = 2000L

        // Read stream offset.
        private const val OFFSET = 0L

        // Number of elements to read from the stream.
        private const val COUNT = 1L

        // Coroutine dispatcher to process events.
        private val coroutineDispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisLettuceNStreams::class.java)

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
         *
         * @return The redis cluster client instance.
         */
        private fun createRedisClusterClient() =
            RedisClusterClient.create(
                List(NUMBER_OF_REDIS_NODES) {
                    RedisURI.Builder.redis(Environment.getRedisHost(), START_REDIS_PORT + it).build()
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
