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
                neighbors.add(Neighbor(ip, port.toInt()))
            }
        }
    }


    /**
     * Connect to the neighbors
     */
    fun toNeighbors() {
        lock.withLock {
            neighbors.forEach { it.connect(localPort, localIp) }
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