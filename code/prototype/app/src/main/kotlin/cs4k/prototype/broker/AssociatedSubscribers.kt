package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * This class is responsible for managing the association between topics and subscribers.
 * It is thread-safe.
 */
class AssociatedSubscribers {

    // Map that associates a topic with a list of subscribers.
    private val map = mutableMapOf<String, List<Subscriber>>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Returns all subscribers associated with a topic.
     * @param topic the topic to get the subscribers from.
     */
    fun getAll(topic: String) = lock.withLock {
        map[topic] ?: emptyList()
    }

    /**
     * Adds a subscriber to a topic.
     * @param topic the topic to add the subscriber to.
     * @param subscriber the subscriber to add.
     */
    fun addToKey(topic: String, subscriber: Subscriber) = lock.withLock {
        val currentSubscribers = map[topic] ?: emptyList()
        map[topic] = currentSubscribers + subscriber
    }

    /**
     * Removes a subscriber from a topic.
     * @param topic the topic to remove the subscriber from.
     * @param predicate a predicate to determine which subscriber to remove.
     */
    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean) = lock.withLock {
        val currentSubscribers = map[topic]
        val subscriberToRemove = (currentSubscribers?.find { predicate(it) } ?: return@withLock)
        map[topic] = currentSubscribers - subscriberToRemove
    }
}
