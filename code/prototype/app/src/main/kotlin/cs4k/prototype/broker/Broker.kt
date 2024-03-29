package cs4k.prototype.broker

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.core.env.Environment
import org.springframework.stereotype.Component
import java.sql.Connection
import java.util.UUID
import kotlin.concurrent.thread

@Component
class Broker {

    // Channel to listen for notifications.
    private val channel = "share_channel"

    // Map that associates a topic with a list of subscribers.
    private val associatedSubscribers = AssociatedSubscribers()

    // Connection pool.
    private var dataSource: HikariDataSource = createConnectionPool()

    private val retry = Retry()

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
     * @param topic String
     * @param handler the handler to be called when there is a new event.
     * @return the callback to be called when unsubscribing.
     * @throws BrokerTurnOffException if the broker is turned off.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        if (dataSource.isClosed) throw BrokerTurnOffException()

        val subscriber = Subscriber(
            id = UUID.randomUUID(),
            handler = handler
        )
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
     * @param topic String
     * @param message String
     * @param isLastMessage Boolean indicates if the message is the last one.
     * @throws BrokerTurnOffException if the broker is turned off.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        if (dataSource.isClosed) throw BrokerTurnOffException()
        notify(topic, message, isLastMessage)
    }

    /**
     * Shutdown the broker.
     */
    fun shutdown() {
        if (dataSource.isClosed) throw BrokerTurnOffException()

        unListen()
        dataSource.close()
    }

    /**
     * Unsubscribe from a topic.
     * @param topic String
     * @param subscriber [Subscriber]
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
        retry.executeWithRetry("Connection Failed: wait for notifications.") {
            dataSource.connection.use { conn ->
                val pgConnection = conn.unwrap(PGConnection::class.java)

                while (!conn.isClosed) {
                    val newNotifications = pgConnection.getNotifications(0) ?: break
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
        retry.executeWithRetry("Connection Failed: listen for notifications.") {
            dataSource.connection.use { conn ->
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
        retry.executeWithRetry("Connection Failed: unListen for notifications.") {
            dataSource.connection.use { conn ->
                conn.createStatement().use { stm ->
                    stm.execute("unListen $channel;")
                }
            }
            logger.info("unListen channel '{}'", channel)
        }
    }

    /**
     * Notify the topic with the message.
     * @param topic String
     * @param message String
     * @param isLastMessage Boolean indicates if the message is the last one.
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        retry.executeWithRetry("Connection Failed: notify topic.") {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                // conn.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE

                val event = Event(
                    topic = topic,
                    id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage),
                    message = message,
                    isLast = isLastMessage
                )

                // 'select pg_notify()' is used because NOTIFY cannot be used in preparedStatement.
                // query results are ignored, but notifications are still sent.
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
     * If the topic does not exist, return null.
     * @param topic String
     */
    private fun getLastEvent(topic: String): Event? =
        retry.executeWithRetry("Connection Failed: get last event.") {
            dataSource.connection.use { conn ->
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
     * Get the next event id and update the history.
     * If the topic does not exist, insert a new one.
     * If the topic exists, update the existing one.
     * Return the next event id.
     * @param conn Connection
     * @param topic String
     * @param message String
     * @param isLast Boolean indicates if the message is the last one.
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
            return if (rs.next()) rs.getLong("id") else throw IllegalStateException("TODO")
        }
    }

    /**
     * Create the events table if it does not exist.
     */
    private fun createEventsTable() {
        retry.executeWithRetry("Connection Failed: create events table.") {
            dataSource.connection.use { conn ->
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
        // Logger instance for logging Broker class events.
        private val logger = LoggerFactory.getLogger(Broker::class.java)

        // ObjectMapper instance for serializing and deserializing JSON with Kotlin support.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        // Retry mechanism.
        private val retry = Retry()

        /**
         * Create a connection to the database.
         */
        private fun createConnectionPool(): HikariDataSource {
            val config = HikariConfig()
            val dbUrl = System.getenv("DB_URL")
                ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
            return retry.executeWithRetry("Failed to stablish connection.") {
                config.jdbcUrl = dbUrl
                HikariDataSource(config)
            }
        }
    }
}
