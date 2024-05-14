import java.net.ServerSocket
import java.net.Socket
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.SocketTimeoutException
import kotlin.concurrent.thread

fun main() {
    val b = BrokerSockets()
    while (true) {
        val message = readLine() ?: ""
        b.publishMessage(message)
    }
}


fun m3ain() {
    val s = Socket("127.0.0.1", "65500".toInt())
    val out = BufferedWriter(OutputStreamWriter(s.getOutputStream()))
    out.write("atao ze tas ai")
    out.newLine()
    out.flush()
    out.close()
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
            println("Listening on port $assignedPort")
            node.warnNeighbors()
            while (true) {
                val clientSocket = clientServer.accept()
                thread {
                    handleConnection(clientSocket)
                }
            }
        }
    }

    private fun handleConnection(clientSocket: Socket) {
        clientSocket.use {
            val clientInput = BufferedReader(InputStreamReader(clientSocket.getInputStream()))
            val clientOutput = BufferedWriter(OutputStreamWriter(clientSocket.getOutputStream()))
            clientSocket.soTimeout = 1000
            try {
                val message = clientInput.readLine() // Tente ler diretamente
                if (message != null && message.startsWith("new_neighbor")) {
                    val neighbor = message.split(";")[1]
                    node.updateNeighborList(neighbor)
                }
                println("Received message: $message")
            } catch (e: SocketTimeoutException) {
                println("No data received within the timeout period.")
            } catch (e: IOException) {
                println("Error reading from socket: ${e.message}")
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
            println("Broker with port -> $assignedPort")
            println("Neighbourhood Ports: $neighbors")
            node = Node(assignedPort)
            if (neighbors.first() != "") {
                node.addNeighbors(neighbors.joinToString(","))
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