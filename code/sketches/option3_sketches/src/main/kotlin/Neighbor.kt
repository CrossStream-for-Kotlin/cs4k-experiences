import java.io.BufferedWriter
import java.io.IOException
import java.io.OutputStreamWriter
import java.net.Socket


/**
 * Class that represents a neighbor with a pre-established connection.
 */
class Neighbor (private var socket: Socket) {
    init {
        socket.keepAlive = true
    }
    private var out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
    /**
     * Send a message to the neighbor using the established connection.
     */
    fun sendMessage(message: String) {
        try {
            if (!isConnected()) {
                disconnect()
               // reconnect()
            }
            println("Sending message to neighbor: $message to port ${socket.port}")
            out.write(message)
            out.newLine()
            out.flush()
        } catch (e: IOException) {
            println("IO Error sending message to neighbor ${socket.inetAddress.hostAddress}:${socket.port} -> ${e.message}")
            disconnect()
        } catch (e: Exception) {
            println("General Error sending message to neighbor ${socket.inetAddress.hostAddress}:${socket.port} -> ${e.message}")
            disconnect()
            throw e
        }
    }

    fun reconnect() {
        disconnect()
        try {
            socket = Socket(socket.inetAddress, socket.port)
            socket.keepAlive = true
            out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
        } catch (e: IOException) {
            println("Failed to reconnect: ${e.message}")
            throw e
        }
    }

    fun disconnect() {
        try {
            out.close()
            socket.close()
        } catch (e: IOException) {
            println("Error closing resources: ${e.message}")
        }
    }


    /**
     * Check if there is an active connection.
     */
    fun isConnected(): Boolean = socket.isConnected && !socket.isClosed
}
