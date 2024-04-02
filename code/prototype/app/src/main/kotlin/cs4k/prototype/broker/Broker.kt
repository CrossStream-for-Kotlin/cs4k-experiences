package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import cs4k.prototype.broker.BrokerException.BrokerDbConnectionException
import cs4k.prototype.broker.BrokerException.BrokerDbLostConnectionException
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.BrokerException.UnexpectedBrokerException
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.util.UUID
import kotlin.concurrent.thread

@Component
class Broker {

    // Channel to listen for notifications.
    private val channel = "share_channel"

    // Map that associates topics with lists of subscribers.
    private val associatedSubscribers = AssociatedSubscribers()

    // Executor.
    private val executor = Executor()

    // Connection pool.
    private val connectionPool = executor.executeWithRetry(BrokerDbConnectionException()) {
        createConnectionPool()
    }

    init {
        // Create the events table if it does not exist.
        createEventsTable()

        // Start a new thread to listen for notifications.
        thread {
            // Listen for notifications.
            listen()
            // Wait for notifications.
            waitForNotification()
        }
    }

    /**
     * Subscribe to a topic.
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The callback to be called when unsubscribing.
     * @throws BrokerTurnOffException if the broker is turned off.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::subscribe.name}.")

        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    fun <K> publish(topic: String, payload: K, isLastMessage: Boolean = false) {
        publish(topic, objectMapper.writeValueAsString(payload), isLastMessage)
    }

    /**
     * Publish a message to a topic.
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerTurnOffException if the broker is turned off.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::publish.name}.")

        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     * @throws BrokerTurnOffException if the broker is turned off.
     */
    fun shutdown() {
        if (connectionPool.isClosed) throw BrokerTurnOffException("Cannot invoke ${::shutdown.name}.")

        unListen()
        connectionPool.close()
    }

    /**
     * Unsubscribe from a topic.
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic) { sub -> sub.id == subscriber.id }
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    /**
     * Wait for notifications.
     * If a new notification arrives, create an event and call the handler of the associated subscribers.
     */
    private fun waitForNotification() {
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                val pgConnection = conn.unwrap(PGConnection::class.java)

                while (!conn.isClosed) {
                    val newNotifications = pgConnection.getNotifications(0)
                        ?: throw UnexpectedBrokerException()
                    newNotifications.forEach { notification ->
                        logger.info("new notification '{}'", notification.parameter)
                        val event = deserialize(notification.parameter)
                        associatedSubscribers
                            .getAll(event.topic)
                            .forEach { subscriber -> subscriber.handler(event) }
                    }
                }
            }
        }
    }

    /**
     * Serialize an event to JSON string.
     * @param event the event to serialize.
     * @return the resulting JSON string.
     */
    private fun serialize(event: Event) = objectMapper.writeValueAsString(event)

    /**
     * Deserialize a JSON string to event.
     * @param payload the JSON string to deserialize.
     * @return the resulting event.
     */
    private fun deserialize(payload: String) = objectMapper.readValue(payload, Event::class.java)

    /**
     * Listen for notifications.
     */
    private fun listen() {
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute("listen $channel;")
                }
            }
            logger.info("listen channel '{}'", channel)
        }
    }

    /**
     * UnListen for notifications.
     */
    private fun unListen() {
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute("unListen $channel;")
                }
            }
            logger.info("unListen channel '{}'", channel)
        }
    }

    /**
     * Notify the topic with the message.
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                conn.autoCommit = false

                val event = Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )

                conn.prepareStatement("select pg_notify(?, ?)").use { stm ->
                    stm.setString(1, channel)
                    stm.setString(2, serialize(event))
                    stm.execute()
                }

                conn.commit()
                conn.autoCommit = true

                logger.info("notify topic '{}' event '{}", topic, event)
            }
        }
    }

    /**
     * Get the last event from the topic.
     * @param topic The topic name.
     * @return The last event of the topic, or null if the event does not exist yet.
     */
    private fun getLastEvent(topic: String): Event? =
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                conn.prepareStatement("select id, message, is_last from events where topic = ? for share;").use { stm ->
                    stm.setString(1, topic)
                    val rs = stm.executeQuery()
                    return@executeWithRetry if (rs.next()) {
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
        }

    /**
     * Get the event id and update the history, i.e.:
     *  - If the topic does not exist, insert a new one.
     *  - If the topic exists, update the existing one.
     * @param conn The connection to be used to interact with the database.
     * @param topic The topic name.
     * @param message The message.
     * @param isLast Indicates if the message is the last one.
     * @return The event id.
     * @throws UnexpectedBrokerException if something unexpected happens.
     */
    private fun getEventIdAndUpdateHistory(conn: Connection, topic: String, message: String, isLast: Boolean): Long {
        conn.prepareStatement(
            """
                insert into events (topic, message, is_last) 
                values (?, ?, ?) 
                on conflict (topic) do update 
                set id = events.id + 1, message = excluded.message, is_last = excluded.is_last
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
     * Create the events table if it does not exist.
     */
    private fun createEventsTable() {
        executor.executeWithRetry(BrokerDbLostConnectionException()) {
            connectionPool.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute(
                        """
                    create table if not exists events (
                        topic varchar(128) primary key, 
                        id integer default 0, 
                        message varchar(512), 
                        is_last boolean default false
                    );
                        """.trimIndent()
                    )
                }
            }
        }
    }

    companion object {
        // Logger instance for logging Broker class information.
        private val logger = LoggerFactory.getLogger(Broker::class.java)

        // ObjectMapper instance for serializing and deserializing JSON.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Create a connection poll for database interactions.
         * @return The connection poll represented by a HikariDataSource instance.
         * @see [HikariCP](https://github.com/brettwooldridge/HikariCP)
         */
        private fun createConnectionPool(): HikariDataSource {
            val hikariConfig = HikariConfig()
            hikariConfig.jdbcUrl = Environment.getDbUrl()
            return HikariDataSource(hikariConfig)
        }
    }
}
