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
        val assignedPort = getAssignedPort()
        thread {
            listen(assignedPort)
        }

    }

    /**
     * Listen to the assigned port
     */
    fun listen(assignedPort: Int) {
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
                    println("Receveid Message: $message")
                }
            }
        }
    }

    /**
     * Get the port assigned by the server
     */
    fun getAssignedPort(): Int {
        Socket(serverHost, serverPort).use { socket ->
            val input = BufferedReader(InputStreamReader(socket.getInputStream()))
            val output = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            val received = input.readLine().split(";")
            val assignedPort = received[0].toInt()
            val neighbors = received[1].split(",")
            println("Broker with port -> $assignedPort")
            println("Neighbourhood Ports: $neighbors")
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
        println("Send message to the neighbourdhood: $message")
        node.sendMessageToNeighbors(message)
    }
}

/**
 * Class that represents a neighbor
 */
class Neighbor(val ip: String, val port: Int) {
    //lateinit var socket: Socket

    /**
     * Connect to the neighbor and informs it about the new neighbor
     */
    fun connect(myPort: Int, myIp: String) {
        try {
            val socket = Socket(ip.trim(), port)
            val out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            out.write("new_neighbor;$myIp:$myPort")
            out.newLine()
            out.flush()
            out.close()
        } catch (e: Exception) {
            println("Erro ao conectar-se ao vizinho $ip:$port -> ${e.message}")
            throw e
        }
    }

    /**
     * Send a message to the neighbor
     */

    fun sendMessage(message: String) {
        try {
            val socket = Socket(ip.trim(), port)
            val out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            out.write(message)
            out.newLine()
            out.flush()
            out.close()
        }catch (e: Exception) {
            println("Erro ao enviar mensagem para o vizinho $ip:$port -> ${e.message}")
            throw e
        }
    }
}


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
