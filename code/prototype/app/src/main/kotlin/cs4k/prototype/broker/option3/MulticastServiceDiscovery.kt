package cs4k.prototype.broker.option3

import org.slf4j.LoggerFactory
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.SocketException
import kotlin.concurrent.thread

/**
 * Responsible for service discovery through multicast.
 *
 * @property associatedPeers
 */
class MulticastServiceDiscovery(
    private val associatedPeers: AssociatedPeers
) {

    // Multicast IP address.
    private val inetAddress = InetAddress.getByName(MULTICAST_IP)

    // Socket address.
    private val inetSocketAddress = InetSocketAddress(inetAddress, MULTICAST_PORT)

    // Multicast socket to send and receive IP multicast packets.
    private val multicastSocket = MulticastSocket(MULTICAST_PORT)

    // Active network interface that supports multicast.
    private val networkInterface = Utils.getActiveMulticastNetworkInterface()

    // Buffer that stores the content of received IP multicast packets.
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    init {
        // Redefine the Time To Live value of IP multicast packets sent.
        // I.e. The maximum number of machine-to-machine hops that packets can make before being discarded.
        multicastSocket.timeToLive = TIME_TO_LIVE

        // Join the multicast group.
        multicastSocket.joinGroup(inetSocketAddress, networkInterface)

        // Announce existence to other nodes.
        announceExistenceToOtherNodes()

        // Start a new thread ...
        thread {
            // ... to listen for IP multicast packets.
            listenMulticastSocket()
        }
    }

    /**
     * Announce the existence of the node by sending a datagram packet.
     */
    private fun announceExistenceToOtherNodes() {
        val messageBytes = MESSAGE.toByteArray()
        val datagramPacket = DatagramPacket(messageBytes, messageBytes.size, inetSocketAddress)
        multicastSocket.send(datagramPacket)

        logger.info("announce ip '{}'", datagramPacket.address)
    }

    /**
     * Blocks the thread reading the socket and processes the IP multicast packets received.
     */
    private fun listenMulticastSocket() {
        logger.info("start reading socket ip '{}' port '{}'", MULTICAST_IP, MULTICAST_PORT)
        while (!multicastSocket.isClosed) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)

                associatedPeers.addPeer(
                    Peer(
                        inetAddress = receivedDatagramPacket.address,
                        state = PeerState.NOT_CONNECTED
                    )
                )
            } catch (e: SocketException) {
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
