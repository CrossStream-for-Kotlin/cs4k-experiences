package cs4k.prototype.broker.option3

import java.net.InetAddress
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
     * Get a neighbor.
     *
     * @param inetAddress The IP address.
     * @return The set of neighbors.
     */
    fun get(inetAddress: InetAddress) = lock.withLock {
        set.find { it.inetAddress == inetAddress }
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
     * Add new neighbors if they don't exist yet.
     *
     * @param neighbors The neighbors to add.
     */
    fun addAll(neighbors: Set<Neighbor>) {
        lock.withLock {
            set.addAll(neighbors.filter { neighbor -> set.none { it.inetAddress == neighbor.inetAddress } })
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

    /**
     * Update a neighbor.
     *
     * @param neighbor The neighbor to update.
     */
    fun update(neighbor: Neighbor) =
        lock.withLock {
            remove(neighbor)
            add(neighbor)
            get(neighbor.inetAddress)
        }
}
