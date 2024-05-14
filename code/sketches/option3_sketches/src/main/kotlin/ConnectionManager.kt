import java.net.ServerSocket
import java.net.Socket
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Represents the connection manager, responsible to atribute ports to the nodes and send the neighbors ports to the new node
 */
class ConnectionManager {
    val serverPort = 65432
    val portPoolStart = 65500
    var nextAvailablePort = portPoolStart
    val connectionsNodes = mutableListOf<NodeConnection>()
    val lock = ReentrantLock()

    init {
        listen()
    }

    fun listen() {
        ServerSocket(serverPort).use { serverSocket ->
            println("Main Server wiht port $serverPort")
            while (true) {
                val socket = serverSocket.accept()
                val assignedPort = lock.withLock { nextAvailablePort++ }
                val nodeConnection = NodeConnection(socket, assignedPort)
                println("Client connected: ${nodeConnection.address} with port $assignedPort")
                val neighborDetails = connectionsNodes.joinToString(separator = ", ") {
                    "${it.address}:${it.assignedPort}"
                }
                nodeConnection.send(
                    "$assignedPort;${
                        neighborDetails
                    }"
                )
                lock.withLock {
                    connectionsNodes.add(nodeConnection)
                }
            }
        }
    }
}


fun main() {
    ConnectionManager()
}


