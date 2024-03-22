package cs4k.prototype.broker

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.io.IOException
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

@Component
class Notifier {

    init {
        // Create a table to store and update the last id of each "channel".
        createAccumulatorTable()
    }

    private val connection = createConnection()
    private val channel = "sharechannel"

    private class Session(val listener: Listener)

    /**
     * Queue to store the sessions to be monitored.
     */
    private val sessionQueue = LinkedBlockingQueue<Session>(16)


    /**
     * Thread to monitor "sessionQueue" and send messages, when notified, and keep alive to sse emitter.
     */
    private val notifyingThread = thread {
        connection.createStatement().use {
            it.execute("LISTEN sharechannel;")
        }
        waitForNotification()
    }


    /**
     * Monitor "channel" and send messages, when notified
     */
    private fun waitForNotification() {
        // Unwrap connection to PGConnection, mainly to monitor "channels" notifications.
        val pgConnection = connection.unwrap(PGConnection::class.java)
        try {
            while (!connection.isClosed) {
                val newNotifications = pgConnection.getNotifications(0)
                logger.info("listen channel {} pid {} ", channel, pgConnection.backendPID)
                if (newNotifications.isNotEmpty()) {
                    newNotifications.forEach { notification ->
                        val splitPayload = notification.parameter.split("||")
                        val topicReceived = splitPayload[0]
                        //TOPIC||ID||MESSAGE[||done]
                        sessionQueue.filter { it.listener.topic == topicReceived }
                            .forEach { session ->
                                session.listener.callback(
                                    Event2(
                                        topicReceived,
                                        splitPayload[1].toLong(),
                                        splitPayload[2]
                                    ), splitPayload.size > 3 && splitPayload[3] == ("done")
                                )
                            }
                    }

                }
            }
        } catch (ex: IOException) {
            logger.info("sseEmitter closed channel {} pid {}", channel, pgConnection.backendPID)
        }
    }

    /**
     * Listen a "channel".
     * @param listener the listener to listen the "channel".
     */
    fun listen(listener: Listener) {
        logger.info("new listener topic {}", listener.topic)

        // Add to session queue to dedicate a coroutine to monitor "channel" and send messages, when notified, and keep alive to sse emitter.
        sessionQueue.add(Session(listener))
    }

    /**
     * Send a notification to "channel".
     * @param topic the topic to send.
     * @param id the id of the message.
     * @param message the message to send.
     * @param complete if the message is complete.
     */
    fun send(topic: String, id: Long, message: String, complete: Boolean = false) {
        logger.info("From topic [{}] send message [{}] on channel {}", topic, message, channel)
        val payload = if (complete) "$topic||$id||$message||done" else "$topic||$id||$message"
        // Send a notification and close connection.
        createConnection().use {
            // select pg_notify is used because NOTIFY cannot be used in preparedStatement.
            // query results are ignored, but notifications are still sent.
            val stm = it.prepareStatement("select pg_notify(?, ?)")
            stm.setString(1, channel)
            stm.setString(2, payload)
            stm.execute()
        }
    }

    /**
     * UnListen a "channel".
     * @param channel the "channel" to unListen.
     */
    private fun unListen(channel: String) {
        logger.info("remove listener channel {}", channel)

        // Receive the connection used to monitor the "channel", unListen channel and close connection.
        connection.prepareStatement("UNLISTEN ?;").use {
            it.setString(1, channel)
        }
    }

    /**
     * Subscribe to a topic.
     */
    fun subscribe(topic: String, callback: (Event2, toComplete: Boolean) -> Unit) {
        listen(Listener(topic, callback))
    }

    /**
     * Publish a message to a topic.
     */
    fun publish(topic: String, message: String) {
        val id = getEventIdAndUpdateHistory(topic, message)
        send(topic, id, message)
    }

    /**
     * Send a keep alive to a sse emitter.
     * @param sseEmitter the sse emitter to send the keep alive.
     */
    private fun sendKeepAlive(sseEmitter: SseEmitter) {
        logger.info("send keepAlive")
        Event.KeepAliveV1(Instant.now().epochSecond).writeTo(sseEmitter)
    }

    /**
     * Create a table to store the last id of each channel.
     */
    private fun createAccumulatorTable() {
        createConnection().use {
            it.createStatement().execute(
                "create table if not exists accumulator (topic varchar(255), id integer, message varchar(255));"
            )
        }
    }

    /**
     * Get the next id of a "topic", while updating the table of events
     */
    //TODO: Transatction level isolation
    private fun getEventIdAndUpdateHistory(topic: String, message: String): Long {
        val c = createConnection()
        c.prepareStatement("select id from accumulator where topic = ?;").use { stm ->
            stm.setString(1, topic)
            val rs = stm.executeQuery()
            if (rs.next()) {
                val newEventId = rs.getLong("id") + 1
                updateLastEvent(c, newEventId, message, topic)
                return newEventId
            } else {
                insertFirstEventOfTopic(c, message, topic)
                return 0
            }
        }
    }

    /**
     * Inserting event into the table containing the last event of each topic
     */
    private fun insertFirstEventOfTopic(c: Connection, message: String, topic: String) {
        c.prepareStatement(
            "insert into accumulator (topic, id, message) values (?, 0, ?);"
        ).use {
            it.setString(1, topic)
            it.setString(2, message)
            it.executeUpdate()
        }
    }

    /**
     * Updates the last event of topic
     */
    private fun updateLastEvent(c: Connection, id: Long, message: String, topic: String) {
        c.prepareStatement(
            "update accumulator set id = ? , message = ? where topic = ?;"
        ).use { stmUpdate ->
            stmUpdate.setLong(1, id)
            stmUpdate.setString(2, message)
            stmUpdate.setString(3, topic)
            stmUpdate.executeUpdate()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Notifier::class.java)
    }
}

/**
 * Create a JBDC connection.
 */
fun createConnection(): Connection {
    val url = System.getenv("DB_URL")
        ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
    return DriverManager.getConnection(url)
}
