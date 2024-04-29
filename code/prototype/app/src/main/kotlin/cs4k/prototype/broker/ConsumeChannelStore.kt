package cs4k.prototype.broker

import com.rabbitmq.client.Channel
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Container associating all topics with their respective consuming channels.
 */
class ConsumeChannelStore {

    // Map that associates topics with consuming channels
    private val map = HashMap<String, ChannelInfo>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    // Class that represents the information of a channel, namely if they're being analyzed for eventual closure.
    private class ChannelInfo(val channel: Channel, var isBeingAnalyzed: Boolean = false)

    /**
     * Marks the channel as being used for analysis if it hasn't yet.
     * @param topic The topic being consumed.
     * @return true if it wasn't analyzed yet, and it was marked as such by the thread, false if it's already being
     * analyzed by another.
     */
    fun markForAnalysis(topic: String) = lock.withLock {
        val entry = map[topic]
        if (entry != null && !entry.isBeingAnalyzed) {
            entry.isBeingAnalyzed = true
            true
        } else {
            false
        }
    }

    /**
     * Marks the channel as no longer being analyzed.
     * @param topic The topic being consumed.
     */
    fun stopAnalysis(topic: String) = lock.withLock {
        map[topic]?.isBeingAnalyzed = false
    }

    operator fun get(topic: String) = lock.withLock {
        map[topic]?.channel
    }

    operator fun set(topic: String, consumer: Channel?) = lock.withLock {
        if (consumer == null) {
            map.remove(topic)
        } else {
            map[topic] = ChannelInfo(consumer)
        }
    }
}
