package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * This class is responsible for managing the association between topics and [Subscriber]s.
 * It is thread-safe.
 */
class AssociatedSubscribers {

    // Map that associates topics with lists of subscribers.
    private val map = HashMap<String, List<Subscriber>>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get all subscribers associated with a topic.
     * @param topic the topic to get the subscribers from.
     * @return the list of subscribers associated with the topic.
     */
    fun getAll(topic: String) = lock.withLock {
        map[topic] ?: emptyList()
    }

    /**
     * Add a subscriber to a topic.
     * @param topic the topic to add the subscriber to.
     * @param subscriber the subscriber to add.
     */
    fun addToKey(topic: String, subscriber: Subscriber) {
        lock.withLock {
            map.compute(topic) { _, subscribers ->
                subscribers?.let { it + subscriber } ?: listOf(subscriber)
            }
        }
    }

    /**
     * Remove a subscriber from a topic.
     * @param topic the topic to remove the subscriber from.
     * @param predicate a predicate to determine which subscriber to remove.
     */
    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean) {
        lock.withLock {
            map.computeIfPresent(topic) { _, subscribers ->
                val subscriberToRemove = subscribers.find(predicate) ?: return@computeIfPresent subscribers
                (subscribers - subscriberToRemove).ifEmpty { null }
            }
        }
    }
}
