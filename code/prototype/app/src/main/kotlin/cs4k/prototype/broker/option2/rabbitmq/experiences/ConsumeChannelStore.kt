package cs4k.prototype.broker.option2.rabbitmq.experiences

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

    /**
     * Represents information regarding a channel.
     * @property channel Channel in question.
     * @property isBeingAnalyzed If the channel is being checked for potential cleanup.
     */
    private class ChannelInfo(val channel: Channel, var isBeingAnalyzed: Boolean = false)

    /**
     * Marks the channel as being used for analysis for eventual cleanup.
     * @param topic The topic being consumed.
     * @return true if it wasn't analyzed yet, and it was marked as such by the thread, false if it's already being
     * analyzed by another or if the topic in question is not being consumed.
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

    /**
     * Obtain a channel associated with topic.
     * @param topic The topic being consumed.
     * @return The channel where consumption is being done.
     */
    operator fun get(topic: String) = lock.withLock {
        map[topic]?.channel
    }

    /**
     * Associate a topic to a channel or, if channel is null, remove the entry.
     * @param topic The topic being, or formerly was, consumed.
     * @param channel The channel where consumption is being done, or null to signal removal.
     */
    operator fun set(topic: String, channel: Channel?) = lock.withLock {
        if (channel == null) {
            map.remove(topic)
        } else {
            map[topic] = ChannelInfo(channel)
        }
    }
}
