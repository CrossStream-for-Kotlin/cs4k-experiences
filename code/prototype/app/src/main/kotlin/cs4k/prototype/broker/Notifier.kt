package cs4k.prototype.broker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.io.IOException
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

@Component
class Notifier {

    init {
        // Create a table to store and update the last id of each "channel".
        createAccumulatorTable()
    }

    private val connection = createConnection()
    private class Session(val listener: Listener)

    /**
     * Queue to store the sessions to be monitored.
     */
    private val sessionQueue = LinkedBlockingQueue<Session>(16)

    /**
     * Thread to monitor "sessionQueue" and send messages, when notified, and keep alive to sse emitter.
     */
    private val acceptingThread = thread {
       while (true) {
           val session = sessionQueue.poll()
           if (session != null) {
               notify(session.listener)

           }
           Thread.sleep(2000)
       }
    }


    /**
     * Monitor "channel" and send messages, when notified, and keep alive to sse emitter.
     * @param listener the listener to send messages and keep alive.
     */
    private fun notify(listener: Listener) {
        // Unwrap connection to PGConnection, mainly to monitor "channels" notifications.
        val pgConnection = connection.unwrap(PGConnection::class.java)
        try {
            while (!connection.isClosed) {
                val newNotifications = pgConnection.getNotifications(0)
                logger.info("listen channel {} pid {} ", listener.channel, pgConnection.backendPID)
                if (newNotifications.isNotEmpty()) {
                    newNotifications.forEach { notification ->
                        val splitPayload = notification.parameter.split("||")
                        sendMessage(
                            sseEmitter = listener.sseEmitter,
                            name = listener.channel,
                            id = splitPayload[0].toLong(),
                            data = splitPayload[1]
                        )
                        if (splitPayload.contains("done")) {
                            listener.sseEmitter.complete()
                            unListen(listener.channel)
                            return
                        }
                    }
                } else {
                    sendKeepAlive(listener.sseEmitter)
                }
                Thread.sleep(2000)
            }
        } catch (ex: IOException) {
            logger.info("sseEmitter closed channel {} pid {}", listener.channel, pgConnection.backendPID)
        }
    }

    /**
     * Listen a "channel".
     * @param listener the listener to listen the "channel".
     */
    fun listen(listener: Listener) {
        logger.info("new listener channel {}", listener.channel)


        // Listen a "channel".
        connection.prepareStatement("LISTEN ?;").use {
            it.setString(1, listener.channel)
        }

        // Unwrap connection to PGConnection, in this case just for logging.
        val pgConnection = connection.unwrap(PGConnection::class.java)

        // On sse completion ...
        listener.sseEmitter.onCompletion {
            logger.info("on sse completion: channel = {}, pid = {}", listener.channel, pgConnection.backendPID)
            // unListen "channel".
            unListen(listener.channel)
        }

        // On sse error ...
        listener.sseEmitter.onError {
            logger.info("on sse error: channel = {}, pid = {}", listener.channel, pgConnection.backendPID)
            // unListen "channel".
            unListen(listener.channel)
        }

        // Add to session queue to dedicate a coroutine to monitor "channel" and send messages, when notified, and keep alive to sse emitter.
        sessionQueue.add(Session(listener))
    }

    /**
     * Send a notification to "channel".
     * If "complete" is true, the sse emitter will be completed after sending the message. And afterwards, the listener will be removed.
     * @param channel the "channel" to send the notification.
     * @param message the message to send.
     * @param complete if true, the sse emitter will be completed after sending the message.
     */
    fun send(channel: String, message: String, complete: Boolean = false) {
        logger.info("send message [{}] on channel {}", message, channel)
        val id = getNextId(channel)
        val payload = if (complete) "$id||$message||done" else "$id||$message"
        // Send a notification and close connection.
       connection.prepareStatement("NOTIFY ?, ? ;").use {
            it.setString(1, channel)
            it.setString(2, payload)
        }
    }

    /**
     * UnListen a "channel".
     * @param connection the connection used to monitor the "channel".
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
     * Send a message to a sse emitter.
     * @param sseEmitter the sse emitter to send the message.
     * @param id the id of the message.
     * @param name the name of the message.
     * @param data the data of the message.
     */
    private fun sendMessage(sseEmitter: SseEmitter, id: Long, name: String, data: String) {
        logger.info("send message")
        Event.Message(name, id, data).writeTo(sseEmitter)
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
                "create table if not exists accumulator (channel varchar(255), id integer);"
            )
        }
    }

    /**
     * Get the next id of a "channel".
     */
    private fun getNextId(channel: String): Long {
        connection.use { connection ->
            val stm = connection.prepareStatement("select id from accumulator where channel = ?;")
            stm.setString(1, channel)
            val rs = stm.executeQuery()

            return if (rs.next()) {
                val stmUpdate = connection.prepareStatement(
                    "update accumulator set id = id + 1 where channel = ?;"
                )
                stmUpdate.setString(1, channel)
                stmUpdate.executeUpdate()
                rs.getLong("id")
            } else {
                val stmInsert = connection.prepareStatement(
                    "insert into accumulator (channel, id) values (?, 1);"
                )
                stmInsert.setString(1, channel)
                stmInsert.executeUpdate()
                0
            }
        }
    }

    companion object {
        private val coroutineDispatcher = Executors.newFixedThreadPool(2).asCoroutineDispatcher()
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
