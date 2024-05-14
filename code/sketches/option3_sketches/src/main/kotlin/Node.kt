import java.io.BufferedWriter
import java.net.Socket
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Class that represents a node
 */
class Node(private val localPort: Int, private val localIp: String = "127.0.0.1") {
    private val neighbors = mutableListOf<Neighbor>()
    private val lock = ReentrantLock()

    /**
     * Update the list of neighbors
     */
    fun updateNeighborList(neighborData: String) {
        lock.withLock {
            neighborData.split(",").forEach {
                val (ip, port) = it.split(":")
                val newNeighbor = Neighbor(Socket(ip.trim(), port.toInt()))
                neighbors.add(newNeighbor)
            }
        }
    }

    fun addNeighbors(neighborData: String) {
        lock.withLock {
            neighborData.split(",").forEach {
                val (ip, port) = it.split(":")
                val newNeighbor = Neighbor(Socket(ip.trim(), port.toInt()))
                neighbors.add(newNeighbor)
            }
        }
    }

    fun warnNeighbors() {
        lock.withLock {
             neighbors.forEach { it.sendMessage("new_neighbor;$localIp:$localPort") }
        }
    }

  /**
     * Send a message to the neighbors
     */
    fun sendMessageToNeighbors(message: String) {
        lock.withLock {
            neighbors.forEach {
                it.sendMessage(message)
            }
        }
    }
}





