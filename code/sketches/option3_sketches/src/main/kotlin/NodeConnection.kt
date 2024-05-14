/**
 * Represents a connection to a node.
 * @param socket The socket to connect to the node.
 * @param assignedPort The port assigned to the node
 */
class NodeConnection(val socket: Socket, val assignedPort: Int) {
    val input = BufferedReader(InputStreamReader(socket.getInputStream()))
    private val output = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
    val address: String = socket.inetAddress.hostAddress
    val port = assignedPort

    fun send(message: String) {
        output.write("$message\n")
        output.flush()
    }
}