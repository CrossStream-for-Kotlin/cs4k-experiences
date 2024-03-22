package cs4k.prototype.broker

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

@Component
class Notifier {

    private val channel = "sharechannel"
    private val connection = createConnection()
    private val subscriberQueue =
        LinkedBlockingQueue<Subscriber>(100) // TODO: Automatically increase and decrease size.

    init {
        createNotifierTable()

        thread {
            logger.info("listen channel '{}'", channel)
            connection.createStatement().use { it.execute("LISTEN $channel;") }
            waitForNotification()
        }
    }

    fun subscribe(topic: String, handler: (event: Event) -> Unit) {
        logger.info("new subscriber topic '{}'", topic)
        subscriberQueue.add(Subscriber(topic, handler))
        getLastEvent(topic)?.let { event -> handler(event) }
    }

    fun publish(topic: String, message: String) {
        notify(topic, message)
    }

    private fun waitForNotification() {
        val pgConnection = connection.unwrap(PGConnection::class.java)

        while (!connection.isClosed) {
            val newNotifications = pgConnection.getNotifications(0) ?: return
            newNotifications.forEach { notification ->
                val payload = notification.parameter
                logger.info("new notification [{}]", payload)
                val event = createEvent(payload)
                subscriberQueue
                    .filter { subscriber -> subscriber.topic == event.topic }
                    .forEach { subscriber -> subscriber.handler(event) }
            }
        }
    }

    private fun createEvent(payload: String): Event {
        val splitPayload = payload.split("||")
        // TOPIC||ID||MESSAGE||[done]
        return Event(
            topic = splitPayload[0],
            id = splitPayload[1].toLong(),
            message = splitPayload[2],
            isLast = splitPayload.size > 3 && splitPayload[3] == ("done")
        )
    }

    fun notify(topic: String, message: String, isLast: Boolean = false) {
        val connection = createConnection()

        val id = getEventIdAndUpdateHistory(connection, topic, message)
        val payload = if (isLast) "$topic||$id||$message||done" else "$topic||$id||$message"
        logger.info("notify topic '{}' [{}]", topic, payload)

        connection.use {
            // 'select pg_notify()' is used because NOTIFY cannot be used in preparedStatement.
            // query results are ignored, but notifications are still sent.
            val stm = it.prepareStatement("select pg_notify(?, ?)")
            stm.setString(1, channel)
            stm.setString(2, payload)
            stm.execute()
        }
    }

    // TODO: Call unListen.
    private fun unListen() {
        logger.info("unListen channel '{}'", channel)
        connection.createStatement().use { it.execute("UNLISTEN $channel;") }
    }

    // TODO: Transaction level.
    private fun getLastEvent(topic: String): Event? {
        createConnection().prepareStatement("select id, message from notifier where topic = ?;").use { stm ->
            stm.setString(1, topic)
            val rs = stm.executeQuery()
            return if (rs.next()) {
                Event(
                    topic = topic,
                    id = rs.getLong("id"),
                    message = rs.getString("message")
                )
            } else {
                null
            }
        }
    }

    // TODO: Transaction level.
    private fun getEventIdAndUpdateHistory(connection: Connection, topic: String, message: String): Long {
        connection.prepareStatement("select id from notifier where topic = ?;").use { stm ->
            stm.setString(1, topic)
            val rs = stm.executeQuery()
            if (rs.next()) {
                val newEventId = rs.getLong("id") + 1
                updateLastEvent(connection, newEventId, message, topic)
                return newEventId
            } else {
                insertFirstEventOfTopic(connection, message, topic)
                return 0
            }
        }
    }

    // TODO: Transaction level.
    private fun insertFirstEventOfTopic(connection: Connection, message: String, topic: String) {
        connection.prepareStatement(
            "insert into notifier (topic, id, message) values (?, 0, ?);"
        ).use {
            it.setString(1, topic)
            it.setString(2, message)
            it.executeUpdate()
        }
    }

    // TODO: Transaction level.
    private fun updateLastEvent(connection: Connection, id: Long, message: String, topic: String) {
        connection.prepareStatement(
            "update notifier set id = ? , message = ? where topic = ?;"
        ).use { stmUpdate ->
            stmUpdate.setLong(1, id)
            stmUpdate.setString(2, message)
            stmUpdate.setString(3, topic)
            stmUpdate.executeUpdate()
        }
    }

    // TODO: Transaction level.
    private fun createNotifierTable() {
        createConnection().use {
            it.createStatement().execute(
                "create table if not exists notifier (topic varchar(255), id integer, message varchar(255));"
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
