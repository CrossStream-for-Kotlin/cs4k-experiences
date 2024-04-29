package cs4k.prototype.broker

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration

/**
 * Manager of connections created from a single connection.
 * Instead of closing channels, they're reused when a user stops using it.
 */
class ChannelPool(
    private val connection: Connection,
    private val maxChannels: Int = 1023
) : Closeable {

    // There can't be negative channel numbers.
    init {
        require(maxChannels > 0) { "Channel count must be higher than 0." }
    }

    // Lock to turn access to the pool thread-safe.
    private val lock = ReentrantLock()

    // Class detailing information about the channel.
    private class ChannelEntry(
        val channel: Channel,
        var isBeingUsed: Boolean = true,
        var consumerTag: String? = null
    )

    // Flag that dictates if it is closed.
    var isClosed = false
        private set

    // Collection of created channels.
    private val channels: MutableList<ChannelEntry> = mutableListOf()

    // Structure of a request sent by those who ask for a channel.
    private class ChannelRequest(
        val continuation: Continuation<Unit>,
        var channel: Channel? = null
    )

    // List of requests for a channel.
    private val channelRequestList = mutableListOf<ChannelRequest>()

    /**
     * Obtain a new channel. If no channels are available and max capacity is reached, it will passively wait for a new
     * connection given by another thread.
     */
    private suspend fun getChannel(): Channel {
        var myRequest: ChannelRequest? = null
        var channel: Channel? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    if (channels.all { channel -> channel.isBeingUsed }) {
                        if (channels.count() == maxChannels) {
                            val request = ChannelRequest(continuation)
                            myRequest = request
                            channelRequestList.add(request)
                        } else {
                            val myChannel = connection.createChannel()
                            channel = myChannel
                            channels.add(ChannelEntry(myChannel))
                            continuation.resumeWith(Result.success(Unit))
                        }
                    } else {
                        val entry = channels.first { !it.isBeingUsed }
                        entry.isBeingUsed = true
                        channel = entry.channel
                        continuation.resumeWith(Result.success(Unit))
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.channel == null) {
                lock.withLock {
                    channelRequestList.remove(myRequest)
                }
                throw e
            } else {
                return requireNotNull(myRequest?.channel)
            }
        }
        return channel ?: myRequest?.channel!!
    }

    /**
     * Obtain a new channel. If no channels are available and max capacity is reached, it will passively wait for a new
     * connection given by another thread or until timeout is reached.
     */
    fun getChannel(timeout: Duration = Duration.INFINITE): Channel {
        if (isClosed) throw IOException("Pool already closed - cannot obtain new channels")
        return runBlocking {
            var result: Channel? = null
            try {
                withTimeout(timeout) {
                    result = getChannel()
                    result!!
                }
            } catch (e: TimeoutCancellationException) {
                if (result == null) {
                    throw e
                } else {
                    result!!
                }
            }
        }
    }

    /**
     * Marks the channel as being used for consumption of a queue or stream.
     */
    fun registerConsuming(channel: Channel, consumerTag: String) {
        if (isClosed) throw IOException("Pool already closed - cannot obtain new channels")
        lock.withLock {
            val channelInfo = channels.find {
                it.channel.connection.id == channel.connection.id && it.channel.channelNumber == channel.channelNumber
            }
            requireNotNull(channelInfo) { "Channel provided must have been created by the pool" }
            channelInfo.consumerTag = consumerTag
        }
    }

    /**
     * Makes the channel free for the taking, making sure to cancel consumption if it was used for that.
     * If there was someone wanting a channel, it is handed to them instead.
     */
    fun stopUsingChannel(channel: Channel) {
        if (isClosed) return
        lock.withLock {
            val channelInfo = channels.find {
                it.channel.connection.id == channel.connection.id && it.channel.channelNumber == channel.channelNumber
            }
            requireNotNull(channelInfo) { "Channel provided must have been created by the pool" }
            if (channelInfo.consumerTag != null) {
                channelInfo.channel.basicCancel(channelInfo.consumerTag)
                channelInfo.consumerTag = null
            }
            if (channelRequestList.isNotEmpty()) {
                val entry = channelRequestList.removeFirst()
                entry.channel = channel
                entry.continuation.resumeWith(Result.success(Unit))
            } else {
                channelInfo.isBeingUsed = false
            }
        }
    }

    override fun close() = lock.withLock {
        if (!isClosed) {
            isClosed = true
            channelRequestList.forEach {
                it.continuation.resumeWithException(CancellationException("Pools have been closed."))
            }
            connection.close()
            channels.clear()
        }
    }
}