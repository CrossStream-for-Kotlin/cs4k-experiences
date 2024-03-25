package cs4k.prototype.broker

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID
import kotlin.concurrent.thread

@Component
class Notifier {

    // Channel to listen for notifications.
    private val channel = "share_channel"

    // Connection to the database.
    private val connection = createConnection()

    // Map that associates a topic with a list of subscribers.
    private val associatedSubscribers = AssociatedSubscribers()

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
        // Register a shutdown hook to call unListen when the application exits
        Runtime.getRuntime().addShutdownHook(Thread {
            unListen()
        })
    }

    /**
     * Subscribe to a topic.
     * @param topic String
     * @param handler Function1<Event, Unit>
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        val subscriber = Subscriber(
            id = UUID.randomUUID(),
            handler = handler
        )
        associatedSubscribers.addToKey(topic, subscriber)
        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        getLastEvent(topic)?.let { event -> handler(event) }

        return { unsubscribe(topic, subscriber) }
    }

    /**
     * Publish a message to a topic.
     * @param topic String
     * @param message String
     * @param isLast Boolean
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        notify(topic, message, isLastMessage)
    }

    /**
     * Unsubscribe from a topic.
     * @param topic String
     * @param subscriberId UUID
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
        val pgConnection = connection.unwrap(PGConnection::class.java)

        while (!connection.isClosed) {
            val newNotifications = pgConnection.getNotifications(0) ?: break
            newNotifications.forEach { notification ->
                val payload = notification.parameter
                logger.info("new notification [{}]", payload)
                val event = createEvent(payload)
                associatedSubscribers
                    .getAll(event.topic)
                    .forEach { subscriber -> subscriber.handler(event) }
            }
        }
        // connection.close()
        // unListen()
    }

    /**
     * Create an event from the payload.
     * @param payload String
     */
    private fun createEvent(payload: String): Event {
        // TOPIC||ID||MESSAGE||[isLast]
        val splitPayload = payload.split("||")
        return Event(
            topic = splitPayload[0],
            id = splitPayload[1].toLong(),
            message = splitPayload[2],
            isLast = splitPayload.size > 3 && splitPayload[3] == "isLast"
        )
    }

    /**
     * Listen for notifications.
     */
    private fun listen() {
        connection.createStatement().use { stm ->
            stm.execute("listen $channel;")
        }
        logger.info("listen channel '{}'", channel)
    }

    private fun unListen() {
        connection.createStatement().use { stm ->
            stm.execute("unListen $channel;")
        }
        logger.info("unListen channel '{}'", channel)
    }

    /**
     * Notify the topic with the message.
     * @param topic String
     * @param message String
     * @param isLast Boolean
     */
    private fun notify(topic: String, message: String, isLastMessage: Boolean) {
        createConnection().use { conn ->
            conn.autoCommit = false
            // conn.transactionIsolation = Connection.TRANSACTION_SERIALIZABLE

            val id = getEventIdAndUpdateHistory(conn, topic, message, isLastMessage)
            val payload = if (isLastMessage) "$topic||$id||$message||isLast" else "$topic||$id||$message"

            // 'select pg_notify()' is used because NOTIFY cannot be used in preparedStatement.
            // query results are ignored, but notifications are still sent.
            conn.prepareStatement("select pg_notify(?, ?)").use { stm ->
                stm.setString(1, channel)
                stm.setString(2, payload)
                stm.execute()
            }

            conn.commit()
            conn.autoCommit = true

            logger.info("notify topic '{}' [{}]", topic, payload)
        }
    }

    /**
     * Get the last event from the topic.
     * If the topic does not exist, return null.
     * @param topic String
     */
    private fun getLastEvent(topic: String): Event? {
        createConnection().use { conn ->
            conn.prepareStatement("select id, message, is_last from events where topic = ? for share;").use { stm ->
                stm.setString(1, topic)
                val rs = stm.executeQuery()
                return if (rs.next()) {
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
     * Get the last event id and update the history.
     * If the topic does not exist, insert a new one.
     * If the topic exists, update the id and message.
     * If the topic exists and isLast is true, update the isLast.
     * Return the last event id.
     * @param conn Connection
     * @param topic String
     * @param message String
     * @param isLast Boolean
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
        connection.createStatement().use { stm ->
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

    companion object {
        private val logger = LoggerFactory.getLogger(Notifier::class.java)

        private fun createConnection(): Connection {
            val url = System.getenv("DB_URL")
                ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
            return DriverManager.getConnection(url)
        }
    }
}

/**
 * TODO
 * - Error handling;
 * - Clean up subscribers and database;
 * - Change the call to createConnection for each 'getLastEvent' and 'notify' for an elastic connection poll;
 * - Try reduce 'notify' isolation level.
 */
