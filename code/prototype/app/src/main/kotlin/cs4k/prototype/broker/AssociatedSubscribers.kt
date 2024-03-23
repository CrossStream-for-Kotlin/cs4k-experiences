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
        map[topic] = if (currentSubscribers.isEmpty()) listOf(subscriber) else (currentSubscribers as MutableList) + subscriber
    }

    fun removeIf(topic: String, predicate: (Subscriber) -> Boolean) = lock.withLock {
        val toRemove = (map[topic]?.filter { predicate(it) } ?: return@withLock)
        map.remove(topic, toRemove)
    }
}
