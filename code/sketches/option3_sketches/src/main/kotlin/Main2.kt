import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import java.net.SocketException
import kotlin.concurrent.thread

private const val MULTICAST_IP = "228.5.6.7"
private const val MULTICAST_PORT = 6789
private const val INBOUND_BUFFER_SIZE = 2048
private const val TIME_TO_LIVE = 10

fun main() {

    val inetAddress = InetAddress.getByName(MULTICAST_IP)
    val inetSocketAddress = InetSocketAddress(inetAddress, MULTICAST_PORT)
    val multicastSocket = MulticastSocket(MULTICAST_PORT)
    multicastSocket.timeToLive = TIME_TO_LIVE
    val networkInterface = getActiveMulticastNetworkInterface()

    multicastSocket.joinGroup(inetSocketAddress, networkInterface)

    thread {
        val buffer = ByteArray(INBOUND_BUFFER_SIZE)
        while (true) {
            try {
                val receivedDatagramPacket = DatagramPacket(buffer, buffer.size)
                multicastSocket.receive(receivedDatagramPacket)
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
        val messageBytes = message.toByteArray()
        val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, inetSocketAddress)
        multicastSocket.send(datagramPacket)
    }
    multicastSocket.leaveGroup(inetSocketAddress, networkInterface)
    multicastSocket.close()
}

private fun getActiveMulticastNetworkInterface(): NetworkInterface {
    val interfaces = NetworkInterface.getNetworkInterfaces()
    while (interfaces.hasMoreElements()) {
        val networkInterface = interfaces.nextElement()
        if (networkInterface.isUp && !networkInterface.isLoopback && networkInterface.supportsMulticast()) {
            return networkInterface
        }
    }
    throw Exception("There is no active network interface that supports multicast!")
}