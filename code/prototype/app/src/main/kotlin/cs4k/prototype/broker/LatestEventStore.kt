package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Thread-safe storage of the latest events.
 */
class LatestEventStore {

    // Map that associates topics with their respective recent events.
    private val map = HashMap<String, Event>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtaining the most recent event - passively waiting if there isn't any event yet.
     * If timeout is reached and there's no event registered, it will return null.
     */
    fun getLatestEvent(topic: String): Event? = lock.withLock {
        map[topic]
    }

    /**
     * Set the latest event in a topic.
     */
    fun createAndSetLatestEvent(topic: String, message: String, isLast: Boolean) = lock.withLock {
        val id = map[topic]?.id?.plus(1L) ?: 0L
        val recentEvent = Event(topic, id, message, isLast)
        map[topic] = recentEvent
        recentEvent
    }

    /**
     * Removes the event of a given topic.
     */
    fun removeLatestEvent(topic: String) = lock.withLock {
        map.remove(topic)
    }
}
