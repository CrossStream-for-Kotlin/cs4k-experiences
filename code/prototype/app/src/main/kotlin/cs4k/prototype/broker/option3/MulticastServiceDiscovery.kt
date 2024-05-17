package cs4k.prototype.broker.option3

import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import java.net.SocketException

/**
 * Responsible for service discovery through multicast, i.e.:
 *  - Periodic announce existence to neighbors via a multicast datagram packet.
 *  - Process all receive datagram packet to discover neighbors.
 *
 * @property neighbors The set of neighbors.
 * @property selfIp The node's own IP address.
 * @property sendDatagramPacketAgainTime Amount of time, in milliseconds before send another multicast datagram packet.
 */
class MulticastServiceDiscovery(
    private val neighbors: Neighbors,
    private val selfIp: String,
    private val sendDatagramPacketAgainTime: Long = DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME
) {

    // Multicast IP address.
    private val inetAddress = InetAddress.getByName(MULTICAST_IP)

    // Socket address.
    private val inetSocketAddress = InetSocketAddress(inetAddress, MULTICAST_PORT)

    // Multicast socket to send and receive multicast datagram packets.
    private val multicastSocket = MulticastSocket(MULTICAST_PORT)

    // Active network interface that supports multicast.
    private val networkInterface = getActiveMulticastNetworkInterface()

    // Buffer that stores the content of received multicast datagram packets.
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    // Thread to listen for multicast datagram packet.
    private val listenMulticastSocketThread = Thread {
        joinMulticastGroup()
        listenMulticastSocket()
    }

    // Thread to periodic announce existence to neighbors.
    private val periodicAnnounceExistenceToNeighborsThread = Thread {
        periodicAnnounceExistenceToNeighbors()
    }

    init {
        listenMulticastSocketThread.start()
        periodicAnnounceExistenceToNeighborsThread.start()
    }

    /**
     * Join the multicast group.
     */
    private fun joinMulticastGroup() {
        // Redefine the Time To Live value of IP multicast packets sent.
        // I.e. The maximum number of machine-to-machine hops that packets can make before being discarded.
        multicastSocket.timeToLive = TIME_TO_LIVE

        multicastSocket.joinGroup(inetSocketAddress, networkInterface)
    }

    /**
     * Blocks the thread reading the socket and processes multicast datagram packet received.
     */
    private fun listenMulticastSocket() {
        logger.info("[NODE IP '{}'] start reading multicast socket", selfIp)
        while (!listenMulticastSocketThread.isInterrupted && !multicastSocket.isClosed) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)
                val remoteInetAddress = receivedDatagramPacket.address
                if (remoteInetAddress.hostAddress != selfIp) {
                    neighbors.add(Neighbor(remoteInetAddress))
                    logger.info(
                        "[NODE IP '{}'] receive multicast datagram packet from node ip '{}'",
                        selfIp,
                        remoteInetAddress
                    )
                }
            } catch (ex: Exception) {
                if (ex is InterruptedException || ex is SocketException) {
                    logger.info("[NODE IP '{}'] stop reading multicast socket", selfIp)
                    break
                }
                throw ex
            }
        }
    }

    /**
     * Periodic announce the existence to neighbors by sending a multicast datagram packet.
     */
    private fun periodicAnnounceExistenceToNeighbors() {
        while (!periodicAnnounceExistenceToNeighborsThread.isInterrupted && !multicastSocket.isClosed) {
            try {
                val messageBytes = MESSAGE.toByteArray()
                val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, inetSocketAddress)
                multicastSocket.send(datagramPacket)
                logger.info("[NODE IP '{}'] announce node ip '{}'", selfIp, selfIp)
                sleep(sendDatagramPacketAgainTime)
            } catch (ex: Exception) {
                if (ex is InterruptedException || ex is SocketException) {
                    logger.info("[NODE IP '{}'] stop announce existence", selfIp)
                    break
                }
                throw ex
            }
        }
    }

    /**
     * Get one active network interface that supports multicast.
     *
     * @return The first network interface find that supports multicast.
     * @throws Exception If there is no active network interface that supports multicast
     */
    private fun getActiveMulticastNetworkInterface(): NetworkInterface {
        val networkInterfaces = NetworkInterface.getNetworkInterfaces()
        while (networkInterfaces.hasMoreElements()) {
            val networkInterface = networkInterfaces.nextElement()
            if (networkInterface.isUp && networkInterface.supportsMulticast()) {
                return networkInterface
            }
        }
        throw Exception("[NODE IP $selfIp] There is no active network interface that supports multicast!")
    }

    /**
     * Stop service discovery.
     */
    fun stop() {
        listenMulticastSocketThread.interrupt()
        periodicAnnounceExistenceToNeighborsThread.interrupt()
        multicastSocket.leaveGroup(inetSocketAddress, networkInterface)
        multicastSocket.close()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(MulticastServiceDiscovery::class.java)

        private const val MULTICAST_IP = "228.5.6.7"
        private const val MULTICAST_PORT = 6789
        private const val INBOUND_BUFFER_SIZE = 1024
        private const val TIME_TO_LIVE = 10
        private const val MESSAGE = "HELLO"
        private const val DEFAULT_SEND_DATAGRAM_PACKET_AGAIN_TIME = 60_000L
    }
}
