package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.BrokerException.BrokerDbConnectionException
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.BrokerException.DbConnectionPoolSizeException
import cs4k.prototype.broker.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.Environment.getRedisHost
import cs4k.prototype.broker.Environment.getRedisPort
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.XReadArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.support.ConnectionPoolSupport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.exceptions.JedisException
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.concurrent.thread


// V1:
//      - Redis Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)
//      - Jedis client

class BrokerRedisPubSubJedis(private val dbConnectionPoolSize: Int = 10) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Channel prefix and pattern.
    private val channelPrefix = "cs4k-"
    private val channelPattern = "$channelPrefix*"

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
        // Start a new thread to subscribe pattern and process events.
        thread {
            subscribePattern()
        }
    }

    private val singletonJedisPubSub = object : JedisPubSub() {

        override fun onPMessage(pattern: String?, channel: String?, message: String?) {
            if (pattern == null || channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new message '{}' channel '{}'", message, channel)

            val subscribers = associatedSubscribers.getAll(channel.substringAfter(channelPrefix))
            if (subscribers.isNotEmpty()) {
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
     * @return The method to be called when unsubscribing.
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

        publishMessage(topic, message, isLastMessage)
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
                logger.info("psubscribe channel '{}'", channelPattern)
                it.psubscribe(singletonJedisPubSub, channelPattern)
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.resource.use { conn ->
                val event = Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )
                conn.publish(channelPrefix + topic, serialize(event))
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
        // jedis.watch(topic)
        val transaction = conn.multi()
        return try {
            transaction.hsetnx(topic, "${EventProp.ID}", "-1")
            transaction.hincrBy(topic, "${EventProp.ID}", 1)
            transaction.hmset(
                topic,
                hashMapOf("${EventProp.MESSAGE}" to message, "${EventProp.IS_LAST}" to isLast.toString())
            )
            transaction.exec()
                ?.getOrNull(1)
                ?.toString()
                ?.toLong()
                ?: throw UnexpectedBrokerException()
            // ?: throw BrokerOptimisticLockingException()
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String): Event? =
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            val lastEventProps = connectionPool.resource.use { conn -> conn.hgetAll(topic) }
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
        private val logger = LoggerFactory.getLogger(BrokerRedisPubSubJedis::class.java)

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
            return JedisPool(jedisPoolConfig, getRedisHost(), getRedisPort())
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
//      - Redis Pub/Sub using Redis as an in-memory data structure (key-value (hash) pair)
//      - Lettuce client

class BrokerRedisPubSubLettuce(private val dbConnectionPoolSize: Int = 10) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Broker shutdown state.
    private var isShutdown = false

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createRedisClient()
    })

    // Connection to asynchronous subscribe, unsubscribe and publish.
    private val pubSubConnection = retryExecutor.execute({ BrokerDbConnectionException() }, {
        redisClient.connectPubSub()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize, redisClient)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { _ -> true }

    private val singletonRedisPubSubAdapter = object : RedisPubSubAdapter<String, String>() {

        override fun message(channel: String?, message: String?) {
            if (channel == null || message == null) throw UnexpectedBrokerException()
            logger.info("new message '{}' channel '{}'", message, channel)

            val subscribers = associatedSubscribers.getAll(channel)
            if (subscribers.isNotEmpty()) {
                val event = deserialize(message)
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        publishMessage(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        pubSubConnection.removeListener(singletonRedisPubSubAdapter)
        pubSubConnection.close()
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
            onTopicRemove = { unsubscribeTopic(topic) }
        )
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Subscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun subscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            logger.info("subscribe new topic '{}'", topic)
            pubSubConnection.async().subscribe(topic)
        }, retryCondition)
    }

    /**
     * UnSubscribe topic.
     *
     * @param topic The topic name.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun unsubscribeTopic(topic: String) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            logger.info("unsubscribe topic '{}'", topic)
            pubSubConnection.async().unsubscribe(topic)
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
    private fun publishMessage(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            val event = Event(
                topic = topic,
                id = getEventIdAndUpdateHistory(topic, message, isLastMessage),
                message = message,
                isLast = isLastMessage
            )
            pubSubConnection.async().publish(topic, serialize(event))
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
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun getEventIdAndUpdateHistory(topic: String, message: String, isLast: Boolean): Long {
        connectionPool.borrowObject().use { conn ->
            val sync = conn.sync()
            // sync.watch(topic)
            sync.multi()
            try {
                sync.hsetnx(topic, "${EventProp.ID}", "-1")
                sync.hincrby(topic, "${EventProp.ID}", 1)
                sync.hmset(
                    topic,
                    hashMapOf("${EventProp.MESSAGE}" to message, "${EventProp.IS_LAST}" to isLast.toString())
                )
                val exec = sync.exec()
                if (exec == null || exec.isEmpty) throw UnexpectedBrokerException() // throw BrokerOptimisticLockingException()
                return exec[1]
            } catch (e: Exception) {
                sync.discard()
                throw e
            }
        }
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
            val lastEventProps = connectionPool.borrowObject().use { conn ->
                conn.sync().hgetall(topic)
            }
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
        private val logger = LoggerFactory.getLogger(BrokerRedisPubSubLettuce::class.java)

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
         * Create a redis client for database interactions.
         *
         * @return The redis client instance.
         */
        private fun createRedisClient() =
            RedisClient.create(RedisURI.create(getRedisHost(), getRedisPort()))

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

// V3:
//      - Redis Streams (1 stream - n topics)
//      - Lettuce client

class BrokerRedisStreamsDeprecated(private val dbConnectionPoolSize: Int = 10) {

    init {
        // Check database connection pool size.
        checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Broker shutdown state.
    private var isShutdown = false

    // Stream key.
    private val streamKey = "cs4k"

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Redis client.
    private val redisClient = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createRedisClient()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createConnectionPool(dbConnectionPoolSize, redisClient)
    })

    // Retry condition.
    private val retryCondition: (throwable: Throwable) -> Boolean = { _ -> true }

    init {
        // Start a new thread to listen the stream.
        thread {
            listenStream()
        }
    }

    /**
     * Listen the stream.
     *
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun listenStream() {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.borrowObject().use { conn ->
                val sync = conn.sync()
                while (conn.isOpen) {
                    sync.xread(XReadArgs.Builder.block(BLOCK_READ_TIME), XReadArgs.StreamOffset.latest(streamKey))
                        .forEach { msg ->
                            logger.info("new message id '{}'", msg.id)
                            processMessage(msg.body)
                        }
                }
            }
        }, retryCondition)
    }

    /**
     * Process the message, i.e., create an event from the message and call the handler of the associated subscribers.
     *
     * @param body The message properties.
     */
    private fun processMessage(body: Map<String, String>) {
        val topic = body["${EventProp.TOPIC}"] ?: throw UnexpectedBrokerException()
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        addMessageToStream(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

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
        associatedSubscribers.removeIf(topic) { sub -> sub.id == subscriber.id }
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Add a message to the stream.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun addMessageToStream(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            connectionPool.borrowObject().use { conn ->
                val sync = conn.sync()
                val newStreamEntry = mapOf(
                    "${EventProp.TOPIC}" to topic,
                    "${EventProp.ID}" to (getLastEvent(topic, sync)?.id?.plus(1) ?: 0).toString(),
                    "${EventProp.MESSAGE}" to message,
                    "${EventProp.IS_LAST}" to isLastMessage.toString()
                )
                sync.xadd(streamKey, newStreamEntry)
                logger.info("publish topic '{}'", topic)
            }
        }, retryCondition)
    }

    /**
     * Get the last event from the topic.
     *
     * @param topic The topic name.
     * @param sync The RedisCommands API for the current connection.
     * @return The last event of the topic, or null if the topic does not exist yet.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun getLastEvent(topic: String, sync: RedisCommands<String, String>? = null): Event? =
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
            val lastEvents = if (sync != null) {
                getLastStreamMessages(sync)
            } else {
                connectionPool.borrowObject().use { conn -> getLastStreamMessages(conn.sync()) }
            }
            return@execute lastEvents
                .find { msg -> msg.body["${EventProp.TOPIC}"] == topic }    // !!! Go through all the elements? !!!
                ?.let { msg -> createEvent(topic, msg.body) }
        })

    /**
     * Get the latest stream messages.
     *
     * @param sync The RedisCommands API for the current connection.
     * @return The latest stream events, or null if the stream is empty.
     */
    private fun getLastStreamMessages(sync: RedisCommands<String, String>) =
        sync.xrevrange(
            streamKey,
            Range.unbounded(),
            Limit.create(OFFSET, COUNT)    // !!! How many elements to get? !!!
        )

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
        if (id == null || message == null || isLast == null) throw UnexpectedBrokerException()
        return Event(topic, id, message, isLast)
    }

    private companion object {

        // The time that blocks reading the stream.
        const val BLOCK_READ_TIME = 2000L

        // Read stream offset.
        private const val OFFSET = 0L

        // Number of elements to read from the stream.
        private const val COUNT = 500L

        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerRedisStreamsDeprecated::class.java)

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
         * Create a redis client for database interactions.
         *
         * @return The redis client instance.
         */
        private fun createRedisClient(): RedisClient =
            RedisClient.create(RedisURI.create(getRedisHost(), getRedisPort()))

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
    private val redisClient = retryExecutor.execute({ BrokerDbConnectionException() }, {
        createRedisClient()
    })

    // Connection pool.
    private val connectionPool = retryExecutor.execute({ BrokerDbConnectionException() }, {
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
        val coroutines = connection.coroutines()
        var currentStreamMessageId = startStreamMessageId

        while (true) {
            coroutines
                .xread(
                    XReadArgs.Builder.block(BLOCK_READ_TIME),
                    XReadArgs.StreamOffset.from(streamKey.replace("*", topic), currentStreamMessageId),
                ).collect { msg ->
                    logger.info("new message id '{}' topic '{}", msg.id, topic)
                    currentStreamMessageId = msg.id
                    processMessage(topic, msg.body)
                }
        }
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        addMessageToStream(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
     */
    private fun addMessageToStream(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerDbLostConnectionException() }, {
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
     * @throws BrokerDbLostConnectionException If the broker lost connection to the database.
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
        if (id == null || message == null || isLast == null) throw UnexpectedBrokerException()
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
         * Create a redis client for database interactions.
         *
         * @return The redis client instance.
         */
        private fun createRedisClient(): RedisClient {
            val host = Environment.getRedisHost()
            val port = Environment.getRedisPort()
            return RedisClient.create(RedisURI.create(host, port))
        }

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
