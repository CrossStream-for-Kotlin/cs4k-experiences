package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class AssociatedSubscribers {

    private val map = mutableMapOf<String, List<Subscriber>>()
    private val lock = ReentrantLock()

    fun getAll(topic: String) = lock.withLock {
        map[topic] ?: emptyList()
    }

    fun addToKey(topic: String, subscriber: Subscriber) = lock.withLock {
        val currentSubscribers = map[topic] ?: emptyList()
        map[topic] = currentSubscribers + subscriber
    }

    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean) = lock.withLock {
        val currentSubscribers = map[topic]
        val subscriberToRemove = (currentSubscribers?.find { predicate(it) } ?: return@withLock)
        map[topic] = currentSubscribers - subscriberToRemove
    }
}
