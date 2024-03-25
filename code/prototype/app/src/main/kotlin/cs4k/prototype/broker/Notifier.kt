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

    private val channel = "share_channel"
    private val connection = createConnection()
    private val associatedSubscribers = AssociatedSubscribers()

    init {
        createEventsTable()

        thread {
            listen()
            waitForNotification()
        }
    }

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

    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        notify(topic, message, isLastMessage)
    }

    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic) { sub -> sub.id == subscriber.id }
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

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
