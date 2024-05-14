import java.net.ServerSocket
import java.net.Socket
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

fun main() {
    val b = BrokerSockets()
    while (true) {
        val message = readLine() ?: ""
        b.publishMessage(message)
    }
}

class BrokerSockets {
    // port and ip to the server that will assign the ports
    private val serverHost = "127.0.0.1"
    private val serverPort = 65432
    // node that will be created
    private lateinit var node: Node

    init {
        //get the port assigned by the server
        val assignedPort = getAssignedPort()
        //start a new thread to listen to the assigned port, and connect to the neighbors and warn them about the new neighbor
        thread {
            listen(assignedPort)
        }

    }

    /**
     * Listen to the assigned port
     */
    fun listen(assignedPort: Int) {
        //create a server socket to listen to the assigned port
        ServerSocket(assignedPort).use { clientServer ->
            node.toNeighbors()
            while (true) {
                val clientSocket = clientServer.accept()
                clientSocket.use {
                    val clientInput = BufferedReader(InputStreamReader(clientSocket.getInputStream()))
                    val clientOutput = BufferedWriter(OutputStreamWriter(clientSocket.getOutputStream()))

                    val message = clientInput.readLine()
                    if (message.startsWith("new_neighbor")) {
                        val neighbor = message.split(";")[1]
                        node.updateNeighborList(neighbor)
                    }
                    logger.info("Received message: $message")
                }
            }
        }
    }

    /**
     * Get the port assigned by the server, and the neighbors already assigned
     */
    fun getAssignedPort(): Int {
        Socket(serverHost, serverPort).use { socket ->
            val input = BufferedReader(InputStreamReader(socket.getInputStream()))
            val output = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            val received = input.readLine().split(";")
            val assignedPort = received[0].toInt()
            val neighbors = received[1].split(",")
            logger.info("Broker with port -> $assignedPort")
            logger.info("Neighbourhood Ports: $neighbors")
            node = Node(assignedPort)
            if (neighbors.first() != "") {
                node.updateNeighborList(neighbors.joinToString(","))
            }
            return assignedPort
        }
    }

    /**
     * Publish a message to the neighborhood
     */
    fun publishMessage(message: String) {
        logger.info("Send message to the neighbourdhood: $message")
        node.sendMessageToNeighbors(message)
    }

    companion object{
        val logger = Logger.getLogger(BrokerSockets::class.java.name)
    }
}
