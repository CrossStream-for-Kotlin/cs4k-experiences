package cs4k.prototype.broker.option3

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Responsible for storing information about neighbors on the network.
 */
class Neighbors {

    // The hash set of neighbors.
    private val set = hashSetOf<Neighbor>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get all neighbors.
     *
     * @return The set of neighbors.
     */
    fun getAll() = lock.withLock {
        set.toSet()
    }

    /**
     * Add a neighbor if it doesn't exist yet.
     *
     * @param neighbor The neighbor to add.
     */
    fun add(neighbor: Neighbor) {
        lock.withLock {
            if (set.none { it.inetAddress == neighbor.inetAddress }) {
                set.add(neighbor)
            }
        }
    }

    /**
     * Remove a neighbor if it exists.
     *
     * @param neighbor The neighbor to remove.
     */
    fun remove(neighbor: Neighbor) {
        lock.withLock {
            set.removeIf { it.inetAddress == neighbor.inetAddress }
        }
    }
}
