package cs4k.prototype.broker.option3

import org.slf4j.LoggerFactory
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.SocketException
import kotlin.concurrent.thread

/**
 * Responsible for service discovery through multicast, i.e.:
 *  - Announce existence to neighbors via a multicast datagram packet.
 *  - Process all receive datagram packet to discover neighbors.
 *
 * @property Neighbors The set of neighbors.
 */
class MulticastServiceDiscovery(
    private val neighbors: Neighbors
) {

    // Multicast IP address.
    private val inetAddress = InetAddress.getByName(MULTICAST_IP)

    // Socket address.
    private val inetSocketAddress = InetSocketAddress(inetAddress, MULTICAST_PORT)

    // Multicast socket to send and receive multicast datagram packets.
    private val multicastSocket = MulticastSocket(MULTICAST_PORT)

    // Active network interface that supports multicast.
    private val networkInterface = Utils.getActiveMulticastNetworkInterface()

    // Buffer that stores the content of received multicast datagram packets.
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    init {
        // Redefine the Time To Live value of IP multicast packets sent.
        // I.e. The maximum number of machine-to-machine hops that packets can make before being discarded.
        multicastSocket.timeToLive = TIME_TO_LIVE

        // Join the multicast group.
        multicastSocket.joinGroup(inetSocketAddress, networkInterface)

        // Announce existence to neighbors.
        announceExistenceToNeighbors()

        // Start a new thread ...
        thread {
            // ... to listen for multicast datagram packet.
            listenMulticastSocket()
        }
    }

    /**
     * Announce the existence to neighbors by sending a multicast datagram packet.
     */
    private fun announceExistenceToNeighbors() {
        val messageBytes = MESSAGE.toByteArray()
        val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, inetSocketAddress)
        multicastSocket.send(datagramPacket)

        logger.info("announce ip '{}'", datagramPacket.address)
    }

    /**
     * Blocks the thread reading the socket and processes multicast datagram packet received.
     */
    private fun listenMulticastSocket() {
        logger.info("start reading socket ip '{}' port '{}'", MULTICAST_IP, MULTICAST_PORT)
        while (!multicastSocket.isClosed) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)

                neighbors.add(
                    Neighbor(
                        inetAddress = receivedDatagramPacket.address,
                        relationState = NeighborRelationState.NOT_CONNECTED
                    )
                )
            } catch (e: SocketException) {
                logger.info("stop reading socket ip '{}' port '{}", MULTICAST_IP, MULTICAST_PORT)
                break
            }
        }
    }

    /**
     * Stop service discovery.
     */
    fun stop() {
        multicastSocket.close()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(MulticastServiceDiscovery::class.java)

        private const val MULTICAST_IP = "228.5.6.7"
        private const val MULTICAST_PORT = 6789
        private const val INBOUND_BUFFER_SIZE = 1024
        private const val TIME_TO_LIVE = 10
        private const val MESSAGE = "HELLO"
    }
}
