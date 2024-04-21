package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Container associating all topics with their respective consumers.
 */
class TopicOffsets {

    // Map that associates topics with consumers
    private val map = HashMap<String, Long>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtain all topics that are being consumed.
     */
    fun getAllTopics() = lock.withLock {
        map.keys.toList()
    }

    /**
     * Obtain the last offset read from stream.
     * @param topic The topic of consumption.
     * @return The last offset read.
     */
    fun getOffset(topic: String) = lock.withLock {
        map[topic] ?: 0L
    }

    /**
     * Define the last offset read from stream
     * @param topic The topic that is being consumed.
     * @param offset The last offset read.
     *
     */
    fun setOffset(topic: String, offset: Long) = lock.withLock {
        map[topic] = offset
    }
}
