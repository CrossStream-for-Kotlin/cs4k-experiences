import java.net.DatagramPacket
import java.net.InetAddress
import java.net.MulticastSocket
import java.net.SocketException
import kotlin.concurrent.thread

private const val MULTICAST_IP = "228.5.6.7"
private const val MULTICAST_PORT = 6789

/**
 * Adapted from [MulticastSocket](https://docs.oracle.com/javase%2F7%2Fdocs%2Fapi%2F%2F/java/net/MulticastSocket.html).
 */
fun main() {

    val multicastGroup: InetAddress = InetAddress.getByName(MULTICAST_IP)
    val socket = MulticastSocket(MULTICAST_PORT)
    socket.joinGroup(multicastGroup)

    thread {
        val buffer = ByteArray(1024)
        while (true) {
            try {
                val receivedDatagramPacket = DatagramPacket(buffer, buffer.size)
                socket.receive(receivedDatagramPacket)
                val receivedMessage = String(receivedDatagramPacket.data.copyOfRange(0, receivedDatagramPacket.length))
                println("[NEW MESSAGE] $receivedMessage")
            } catch (e: SocketException) {
                break
            }
        }
    }

    println("Enter messages or 'exit' to exit:")
    while (true) {
        val message = readln()
        if (message == "exit") break
        println("[SEND MESSAGE] $message")

        val datagramPacket = DatagramPacket(message.toByteArray(), message.length, multicastGroup, MULTICAST_PORT)
        socket.send(datagramPacket)
    }
    socket.leaveGroup(multicastGroup)
    socket.close()
}
