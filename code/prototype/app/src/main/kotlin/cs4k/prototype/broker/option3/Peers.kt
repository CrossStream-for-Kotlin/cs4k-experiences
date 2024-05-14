package cs4k.prototype.broker.option3

import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Container where this node's peers are stored. Periodically, or when instructed, will do DNS lookups to find its peers.
 * When a new node is announced, its IP will be added here.
 * @property selfInetAddress The IP address of this node, used to filter out its own IP from its peers.
 * @property sharedService The Docker compose service name, used to do DNS queries.
 * @property sharedPort The inbound port shared by all nodes,
 * @property dnsExpiresOn The expiration time, in millis, of the results of the DNS query.
 */
class Peers(
    private val selfInetAddress: InetAddress,
    private val sharedService: String,
    private val sharedPort: Int,
    private val dnsExpiresOn: Long = DEFAULT_DNS_EXPIRES_ON
) {

    // Set of IP sockets addresses of the node's peers.
    private val peers: HashSet<InetSocketAddress> = hashSetOf()

    // Lock to control concurrency.
    private val lock = ReentrantLock()

    // Thread that periodically does DNS queries.
    private val dnsLookupThread = Thread {
        while (true) {
            logger.info("... looking up dns ...")
            dnsLookup()
            sleep(dnsExpiresOn)
        }
    }

    init {
        dnsLookupThread.start()
    }

    /**
     * Makes a DNS query and adds all IP addresses received as peers.
     */
    private fun dnsLookup() = lock.withLock {
        peers.clear()
        try {
            val ipAddresses = InetAddress.getAllByName(sharedService)
            ipAddresses.forEach { ipAddress ->
                when (ipAddress) {
                    is Inet4Address ->
                        if (ipAddress != selfInetAddress) { peers.add(InetSocketAddress(ipAddress, sharedPort))
                        }
                    is InetAddress -> logger.info("not ipv4 address -> {}", ipAddress)
                }
            }
            logger.info("lookup dns, peers -> {}", peers.joinToString(" , "))
        } catch (ex: Exception) {
            logger.error("{}", ex.stackTrace)
        }
    }

    /**
     * Sending the information to all peers.
     * @param socket Where the information will be sent to.
     * @param buf The payload of the datagram sent.
     */
    fun send(socket: DatagramSocket, buf: ByteArray) = lock.withLock {
        peers.forEach { peer ->
            val datagramPacket = DatagramPacket(buf, buf.size, peer)
            socket.send(datagramPacket)
        }
    }

    /**
     * Adding an IP of a new peer to the list of peers.
     * @param address The IP address of the new peer.
     */
    fun addIp(address: InetAddress) {
        lock.withLock {
            peers.add(InetSocketAddress(address, sharedPort))
        }
    }

    /**
     * Releases all resources, namely interrupting the DNS lookup thread.
     */
    fun shutdown() {
        dnsLookupThread.interrupt()
        peers.clear()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(Peers::class.java)

        private const val DEFAULT_DNS_EXPIRES_ON = 5000L
    }
}
