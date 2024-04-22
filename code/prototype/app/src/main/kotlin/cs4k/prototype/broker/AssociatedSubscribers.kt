package cs4k.prototype.broker

import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.concurrent.withLock

/**
 * Responsible for managing the association between topics and [Subscriber]s.
 * It is thread-safe.
 */
class AssociatedSubscribers {

    // Map that associates topics with lists of subscribers.
    private val map = HashMap<String, List<Subscriber>>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get all subscribers associated with a topic.
     *
     * @param topic The topic to get the subscribers from.
     * @return The list of subscribers associated with the topic.
     */
    fun getAll(topic: String) = lock.withLock {
        map[topic] ?: emptyList()
    }

    /**
     * Get all topics
     * @return The list of topics.
     */
    fun getAllKeys() = lock.withLock {
        map.keys.toList()
    }

    fun updateLastEventListened(id: UUID, topic: String, lastId: Long) = lock.withLock {
        map.computeIfPresent(topic) { _, subscribers ->
            subscribers.map { subscriber: Subscriber ->
                if (subscriber.id == id) {
                    subscriber.copy(lastEventNotified = lastId)
                } else {
                    subscriber
                }
            }
        }
    }

    /**
     * Add a subscriber to a topic.
     *
     * @param topic The topic to add the subscriber to.
     * @param subscriber The subscriber to add.
     */
    fun addToKey(topic: String, subscriber: Subscriber) {
        lock.withLock {
            map.compute(topic) { _, subscribers ->
                subscribers?.let { it + subscriber } ?: listOf(subscriber)
            }
        }
    }

    /**
     * Add a subscriber to a topic.
     *
     * @param topic The topic to add the subscriber to.
     * @param subscriber The subscriber to add.
     */
    fun addToKey(topic: String, subscriber: Subscriber, onTopicAdd: () -> Unit = {}) {
        var newTopic = false
        lock.withLock {
            map.compute(topic) { _, subscribers ->
                if (subscribers == null) newTopic = true
                subscribers?.let { it + subscriber } ?: listOf(subscriber)
            }
        }
        if (newTopic) onTopicAdd()
    }

    /**
     * Remove a subscriber from a topic.
     *
     * @param topic The topic to remove the subscriber from.
     * @param predicate A predicate to determine which subscriber to remove.
     */
    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean) {
        lock.withLock {
            map.computeIfPresent(topic) { _, subscribers ->
                val subscriberToRemove = subscribers.find(predicate) ?: return@computeIfPresent subscribers
                (subscribers - subscriberToRemove).ifEmpty {
                    null
                }
            }
        }
    }

    /**
     * Remove a subscriber from a topic.
     *
     * @param topic The topic to remove the subscriber from.
     * @param predicate A predicate to determine which subscriber to remove.
     */
    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean, onTopicRemove: () -> Unit = {}) {
        var topicGone = false
        lock.withLock {
            map.computeIfPresent(topic) { _, subscribers ->
                val subscriberToRemove = subscribers.find(predicate) ?: return@computeIfPresent subscribers
                (subscribers - subscriberToRemove).ifEmpty {
                    topicGone = true
                    null
                }
            }
        }
        if (topicGone) onTopicRemove()
    }
}
