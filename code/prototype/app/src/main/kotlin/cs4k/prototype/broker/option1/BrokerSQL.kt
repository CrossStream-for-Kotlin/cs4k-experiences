package cs4k.prototype.broker.option1

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException.BrokerConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.common.Utils
import cs4k.prototype.broker.option1.ChannelCommandOperation.Listen
import cs4k.prototype.broker.option1.ChannelCommandOperation.UnListen
import org.postgresql.PGConnection
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.SQLException
import java.util.UUID
import kotlin.concurrent.thread

// @Component
class BrokerSQL(
    private val dbConnectionPoolSize: Int = Utils.DEFAULT_DB_CONNECTION_POOL_SIZE
) : Broker {

    init {
        // Check database connection pool size.
        Utils.checkDbConnectionPoolSize(dbConnectionPoolSize)
    }

    // Shutdown state.
    private var isShutdown = false

    // Channel to listen for notifications.
    private val channel = "cs4k_share_channel"

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
        !(throwable is SQLException && connectionPool.isClosed)
    }

    init {
        // Create the events table if it does not exist.
        createEventsTable()

        // Start a new thread to ...
        thread {
            // ... listen for notifications and ...
            listen()
            // ... wait for notifications.
            waitForNotification()
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}'", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        notify(topic, message, isLastMessage)
    }

    override fun shutdown() {
        if (isShutdown) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        isShutdown = true
        unListen()
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
        logger.info("unsubscribe topic '{}' id '{}'", topic, subscriber.id)
    }

    /**
     * Wait for notifications.
     * If a new notification arrives, create an event and deliver it to subscribers.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun waitForNotification() {
        try {
            retryExecutor.execute({ BrokerLostConnectionException() }, {
                connectionPool.connection.use { conn ->
                    val pgConnection = conn.unwrap(PGConnection::class.java)

                    while (!conn.isClosed) {
                        val newNotifications = pgConnection.getNotifications(BLOCK_UNTIL_NEW_NOTIFICATIONS)
                            ?: throw UnexpectedBrokerException()
                        newNotifications.forEach { notification ->
                            if (notification.name == channel) {
                                logger.info(
                                    "new event '{}' backendPid '{}'",
                                    notification.parameter,
                                    pgConnection.backendPID
                                )
                                val event = BrokerSerializer.deserializeEventFromJson(notification.parameter)
                                deliverToSubscribers(event)
                            }
                        }
                    }
                }
            }, retryCondition)
        } catch (ex: PSQLException) {
            if (!retryCondition(ex)) return
            throw ex
        }
    }

    /**
     * Deliver event to subscribers, i.e., call all subscriber handlers of the event topic.
     */
    private fun deliverToSubscribers(event: Event) {
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Listen for notifications.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun listen() = Listen.execute()

    /**
     * UnListen for notifications.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun unListen() = UnListen.execute()

    /**
     * Execute the ChannelCommandOperation.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun ChannelCommandOperation.execute() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute("$this $channel;")
                }
            }
            logger.info("$this channel '{}'", channel)
        }, retryCondition)
    }

    /**
     * Notify the topic with the message.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                try {
                    conn.autoCommit = false

                    val eventJson = BrokerSerializer.serializeEventToJson(
                        Event(
                            topic = topic,
                            id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                            message = message,
                            isLast = isLastMessage
                        )
                    )

                    conn.prepareStatement("select pg_notify(?, ?)").use { stm ->
                        stm.setString(1, channel)
                        stm.setString(2, eventJson)
                        stm.execute()
                    }

                    conn.commit()
                    conn.autoCommit = true

                    logger.info("notify topic '{}' event '{}'", topic, eventJson)
                } catch (e: SQLException) {
                    conn.rollback()
                    throw e
                }
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
    private fun getEventIdAndUpdateHistory(conn: Connection, topic: String, message: String, isLast: Boolean): Long {
        conn.prepareStatement(
            """
                insert into cs4k.events (topic, message, is_last) 
                values (?, ?, ?) 
                on conflict (topic) do update 
                set id = cs4k.events.id + 1, message = excluded.message, is_last = excluded.is_last
                returning id;
            """.trimIndent()
        ).use { stm ->
            stm.setString(1, topic)
            stm.setString(2, message)
            stm.setBoolean(3, isLast)
            val rs = stm.executeQuery()
            return if (rs.next()) rs.getLong("id") else throw UnexpectedBrokerException()
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
            connectionPool.connection.use { conn ->
                conn.prepareStatement("select id, message, is_last from cs4k.events where topic = ?;").use { stm ->
                    stm.setString(1, topic)
                    val rs = stm.executeQuery()
                    return@execute if (rs.next()) {
                        Event(
                            topic = topic,
                            id = rs.getLong("id"),
                            message = rs.getString("message"),
                            isLast = rs.getBoolean("is_last")
                        )
                    } else {
                        null
                    }
                }
            }
        }, retryCondition)

    /**
     * Create the events table if it does not exist.
     *
     * @throws BrokerLostConnectionException If the broker lost connection to the database.
     */
    private fun createEventsTable() {
        retryExecutor.execute({ BrokerLostConnectionException() }, {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute(
                        """
                            create schema if not exists cs4k;
                            create table if not exists cs4k.events (
                                topic varchar(128) primary key, 
                                id integer default 0, 
                                message varchar(512), 
                                is_last boolean default false
                            );
                        """.trimIndent()
                    )
                }
            }
        }, retryCondition)
    }

    private companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(BrokerSQL::class.java)

        // Block until new notifications arrive, using the value '0'.
        private const val BLOCK_UNTIL_NEW_NOTIFICATIONS = 0

        /**
         * Create a connection poll for database interactions.
         *
         * @param dbConnectionPoolSize The maximum size that the pool is allowed to reach.
         * @return The connection poll represented by a HikariDataSource instance.
         * @see [HikariCP](https://github.com/brettwooldridge/HikariCP)
         */
        private fun createConnectionPool(dbConnectionPoolSize: Int): HikariDataSource {
            val hikariConfig = HikariConfig()
            hikariConfig.jdbcUrl = Environment.getPostgreSQLDbUrl()
            hikariConfig.maximumPoolSize = dbConnectionPoolSize
            return HikariDataSource(hikariConfig)
        }
    }
}
