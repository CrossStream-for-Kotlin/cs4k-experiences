package cs4k.prototype.broker.option2.redis

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
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.sync.RedisCommands
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

// V4:
//      - Redis Streams (n streams - n topics)
//      - Lettuce client

class BrokerRedisStreams(private val dbConnectionPoolSize: Int = 10) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Broker shutdown state.
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
        createRedisClient()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerException.BrokerConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize, redisClient)
    })

    // Connection to read streams.
    private val connection = connectionPool.borrowObject()

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { _ -> true }

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
    // Todo: Adds Retry.
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
        lastMessage?.let { msg -> handler(createEvent(topic, msg.body)) }

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber) {
            listeningTopic.add(topic, lastMessage?.id ?: "0")
        }
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

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
                    "${EventProp.MESSAGE}" to message,
                    "${EventProp.ID}" to (getLastEvent(topic, sync)?.let { it.id + 1 } ?: 0).toString(),
                    "${EventProp.IS_LAST}" to isLastMessage.toString()
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
    private fun getLastEvent(topic: String, sync: RedisCommands<String, String>): Event? =
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
     * @param body The message properties.
     * @return The resulting event.
     */
    private fun createEvent(topic: String, body: Map<String, String>): Event {
        val id = body["${EventProp.ID}"]?.toLong()
        val message = body["${EventProp.MESSAGE}"]
        val isLast = body["${EventProp.IS_LAST}"]?.toBoolean()
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
        private val logger = LoggerFactory.getLogger(BrokerRedisStreams::class.java)

        // Minimum database connection pool size allowed.
        const val MIN_DB_CONNECTION_POOL_SIZE = 2

        // Maximum database connection pool size allowed.
        const val MAX_DB_CONNECTION_POOL_SIZE = 100

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
    }
}
