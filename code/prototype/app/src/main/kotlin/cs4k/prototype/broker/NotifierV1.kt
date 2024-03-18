package cs4k.prototype.broker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.mapTo
import org.postgresql.PGConnection
import org.postgresql.ds.PGSimpleDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.io.IOException
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

@Component
class NotifierV1 {

    init {
        // Create a table to store and update the last id of each channel.
        createAccumulatorTable()
    }

    /**
     * Connection.
     */
    private val handle = createJdbi().open()

    /**
     * Queue to store the listeners to be monitored.
     */
    private val listenerQueue = LinkedBlockingQueue<Listener>(16)

    /**
     * Thread to monitor "listenerQueue" and send messages, when notified, to sse emitter.
     */
    private val acceptingThread = thread {
        runBlocking {
            acceptingLoop(this)
        }
    }

    /**
     * Monitor "listenerQueue" and send messages, when notified, to sse emitter.
     */
    private suspend fun acceptingLoop(scope: CoroutineScope) {
        while (true) {
            val listener = listenerQueue.poll()
            if (listener != null) {
                scope.launch(coroutineDispatcher) {
                    notify(listener)
                }
            }
            delay(2000)
        }
    }

    /**
     * Monitor channel and send messages, when notified, to sse emitter.
     * @param listener the listener to send messages.
     */
    private suspend fun notify(listener: Listener) {
        try {
            val pgConnection = handle.connection.unwrap(PGConnection::class.java)
            while (true) {
                val newNotifications = pgConnection.getNotifications(0) ?: return
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
            }
        } catch (ex: IOException) {
            logger.info("sseEmitter closed channel {}", listener.channel)
        }
    }

    /**
     * Listen a channel.
     * @param listener the listener to listen the channel.
     */
    fun listen(listener: Listener) {
        logger.info("new listener channel {}", listener.channel)

        // Listen a channel.
        handle.inTransaction<Unit, Exception> { handle ->
            handle.createQuery("LISTEN :channel;")
                .bind("channel", listener.channel)
        }

        // On sse completion ...
        listener.sseEmitter.onCompletion {
            // unListen channel.
            unListen(listener.channel)
        }

        // On sse error ...
        listener.sseEmitter.onError {
            // unListen channel.
            unListen(listener.channel)
        }

        // Add to "listenerQueue" to dedicate a coroutine to monitor "channel" and send messages, when notified, to sse emitter.
        listenerQueue.add(listener)
    }

    /**
     * Send a notification to channel.
     * If "complete" is true, the sse emitter will be completed after sending the message. And afterward, the listener will be removed.
     * @param channel the "channel" to send the notification.
     * @param message the message to send.
     * @param complete if true, the sse emitter will be completed after sending the message.
     */
    fun send(channel: String, message: String, complete: Boolean = false) {
        logger.info("send message [{}] on channel {}", message, channel)

        val id = getNextId(channel)
        val payload = if (complete) "$id||$message||done" else "$id||$message"

        handle.inTransaction<Unit, Exception> { handle ->
            handle.createQuery("NOTIFY :channel, :payload;")
                .bind("channel", channel)
                .bind("payload", payload)
        }
    }

    /**
     * UnListen a channel.
     * @param channel the channel to unListen.
     */
    private fun unListen(channel: String) {
        logger.info("remove listener channel {}", channel)

        handle.inTransaction<Unit, Exception> { handle ->
            handle.createQuery("UNLISTEN :channel;")
                .bind("channel", channel)
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
     * Get the next id of a channel.
     */
    private fun getNextId(channel: String): Long {
        return handle.inTransaction<Long, Exception> { handle ->
            val id = handle.createQuery("select id from accumulator where channel = :channel;")
                .bind("channel", channel)
                .mapTo<Long>()
                .singleOrNull()

            if (id != null) {
                handle.createUpdate("update accumulator set id = id + 1 where channel = :channel;")
                    .bind("channel", channel)
                    .execute()
                id
            } else {
                handle.createUpdate("insert into accumulator (channel, id) values (:channel, 1);")
                    .bind("channel", channel)
                    .execute()
                0
            }
        }
    }

    /**
     * Create a table, if not exists, to store the last id of each channel.
     */
    private fun createAccumulatorTable() {
        createJdbi().inTransaction<Unit, Exception> { handle ->
            handle.createUpdate("create table if not exists accumulator (channel varchar(255), id integer);")
                .execute()
        }
    }

    companion object {
        private val coroutineDispatcher = Executors.newFixedThreadPool(2).asCoroutineDispatcher()
        private val logger = LoggerFactory.getLogger(Notifier::class.java)
    }
}

/**
 * Create a JDBI object.
 */
private fun createJdbi(): Jdbi =
    Jdbi.create(
        PGSimpleDataSource().apply {
            val dbUrl = System.getenv("DB_URL") ?: throw Exception("Missing env var DB_URL")
            setURL(dbUrl)
        }
    )
