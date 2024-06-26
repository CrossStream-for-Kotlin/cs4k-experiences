package cs4k.prototype.broker.option2.redis.experiences

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.common.Utils
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.XReadArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
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

// [NOTE] Discontinued, mainly, because:
//      - One stream per topic means that the number of streams can scale uncontrollably.
//      - Possible deadlocks.
//      - Some indeterminate behavior.

// - Lettuce Java client
// - Redis Streams (n streams - n topics)
// - Support for Redis Cluster

// @Component
class BrokerRedisLettuceNStreams(
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
) : Broker {

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
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
    private val redisClient = retryExecutor.execute({ BrokerConnectionException() }, {
        createRedisClient()
        // createRedisClusterClient()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize, redisClient)
        // createClusterConnectionPool(dbConnectionPoolSize, redisClient)
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

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val lastMessage = getLastStreamMessage(topic)
        lastMessage?.let { msg -> handler(createEvent(topic, msg.body)) }

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            listeningTopic.add(topic, lastMessage?.id ?: "0")
        }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        addMessageToStream(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        isShutdown = true
        connection.close()
        connectionPool.close()
        redisClient.shutdown()
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
            delay(LISTEN_TOPICS_LOOP_DELAY)
        }
    }

    /**
     * Listen the stream associated to topic.

     * @param topic The topic name.
     * @param startStreamMessageId The identifier of the last message read from stream.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
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
     * @param payload The message properties.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun processMessage(topic: String, payload: Map<String, String>) {
        val subscribers = associatedSubscribers.getAll(topic)
        if (subscribers.isNotEmpty()) {
            val event = createEvent(topic, payload)
            subscribers.forEach { subscriber -> subscriber.handler(event) }
        }
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

                repeat(NUMBER_OF_ATTEMPTS_TO_ACQUIRE_LOCK) {
                    if (sync.setnx("$LOCK_PREFIX$topic", "_")) {
                        val newStreamEntry = mapOf(
                            Event.Prop.MESSAGE.key to message,
                            Event.Prop.ID.key to (getEventId(topic, sync)).toString(),
                            Event.Prop.IS_LAST.key to isLastMessage.toString()
                        )
                        val id = sync.xadd(streamKey.replace("*", topic), newStreamEntry)
                        logger.info("publish topic '{}' id '{}", topic, id)

                        sync.del("$LOCK_PREFIX$topic")
                        return@use
                    }
                }
                throw UnexpectedBrokerException()
            }
        }, retryCondition)
    }

    /**
     * Get the event identifier.
     *
     * @param topic The topic name.
     * @param sync The RedisCommands API for the current connection.
     * @return The event identifier.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getEventId(topic: String, sync: RedisCommands<String, String>) =
        getLastStreamMessage(topic, sync)
            ?.body
            ?.get(Event.Prop.ID.key)
            ?.toLongOrNull()
            ?.plus(1)
            ?: 0

    /**
     * Get the latest stream message.
     *
     * @param topic The topic name.
     * @return The latest stream message, or null if the stream is empty.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastStreamMessage(topic: String) =
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.borrowObject().use { conn ->
                getLastStreamMessage(topic, conn.sync())
            }
        }, retryCondition)

    /**
     * Get the latest stream message.
     *
     * @param topic The topic name.
     * @param sync The RedisCommands API for the current connection.
     * @return The latest stream message, or null if the stream is empty.
     */
    private fun getLastStreamMessage(topic: String, sync: RedisCommands<String, String>) =
        sync.xrevrange(
            streamKey.replace("*", topic),
            Range.unbounded(),
            Limit.create(OFFSET, COUNT)
        ).firstOrNull()

    /**
     * Create an event.
     *
     * @param topic The topic of the message.
     * @param payload The message properties.
     * @return The resulting event.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun createEvent(topic: String, payload: Map<String, String>): Event {
        val id = payload[Event.Prop.ID.key]?.toLong()
        val message = payload[Event.Prop.MESSAGE.key]
        val isLast = payload[Event.Prop.IS_LAST.key]?.toBoolean()
        if (id == null || message == null || isLast == null) throw UnexpectedBrokerException()
        return Event(topic, id, message, isLast)
    }

    private companion object {

        // Listen topics loop.
        private const val LISTEN_TOPICS_LOOP_DELAY = 500L

        // The time that blocks reading the stream.
        private const val BLOCK_READ_TIME = 5000L

        // Read stream offset.
        private const val OFFSET = 0L

        // Number of elements to read from the stream.
        private const val COUNT = 1L

        // Maximum number of attempts to acquire the lock.
        private const val NUMBER_OF_ATTEMPTS_TO_ACQUIRE_LOCK = 250

        // The name of prefix lock.
        private const val LOCK_PREFIX = "lock-"

        // Coroutine dispatcher to process events.
        private val coroutineDispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisLettuceNStreams::class.java)

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
                List(6) {
                    RedisURI.Builder.redis(Environment.getRedisHost(), 7000 + it).build()
                }
            )

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
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
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
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
