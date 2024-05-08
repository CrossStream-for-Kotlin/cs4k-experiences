package cs4k.prototype.broker.option2.rabbitmq

import cs4k.prototype.broker.common.Event
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
     * @param topic The topic of the event.
     * @return The latest event of given topic.
     */
    fun getLatestEvent(topic: String): Event? = lock.withLock {
        map[topic]
    }

    /**
     * Create and set the latest event in a topic.
     * @param topic The topic of the event.
     * @param message The message of the event.
     * @param isLast If the event in question is the last of a given topic.
     * @return The newly created event.
     */
    fun createAndSetLatestEvent(topic: String, message: String, isLast: Boolean = false) = lock.withLock {
        val id = map[topic]?.id?.plus(1L) ?: 0L
        val recentEvent = Event(topic, id, message, isLast)
        map[topic] = recentEvent
        recentEvent
    }

    /**
     * Removes all stored events.
     */
    fun removeAll() = lock.withLock { map.clear() }
}
