package cs4k.prototype.broker.option3

import cs4k.prototype.broker.common.Environment
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.net.Inet4Address
import java.net.InetAddress
import kotlin.concurrent.thread

/**
 * Responsible for service discovery through Domain Name System (DNS) queries, i.e.:
 *  - Querying the DNS server, giving it a name (that being a Docker service name),
 *  - Adding new entries to the neighbors.
 *
 * @property Neighbors The set of neighbors.
 * @property nodeInetAddress The node's own IP address.
 * @property lookupAgainTime Amount of time, in milliseconds before another DNS query.
 */
class DNSServiceDiscovery(
    private val neighbors: Neighbors,
    private val nodeInetAddress: InetAddress = InetAddress.getByName(Environment.getHost()),
    private val lookupAgainTime: Long = 5000L
) {

    // Thread responsible for making periodic DNS queries.
    private val dnsLookupThread = thread {
        try {
            while (true) {
                logger.info("querying dns server...")
                dnsLookup()
                sleep(lookupAgainTime)
            }
        } catch (e: Exception) {
            if (e is InterruptedException) {
                logger.info("dns lookup interrupted.")
            } else {
                logger.info("dns lookup error: " + e.stackTrace)
            }
        }
    }

    /**
     * Make a DNS query and add all new IP addresses received as peers.
     */
    private fun dnsLookup() {
        val neighborsIp = InetAddress.getAllByName(SERVICE_NAME)
            .filter { it is Inet4Address && it != nodeInetAddress }
            .map { Neighbor(it) }
            .toSet()
        neighbors.addAll(neighborsIp)
        logger.info("dns lookup success: neighbours found -> {}", neighborsIp.joinToString(", "))
    }

    /**
     * Stop service discovery.
     */
    fun stop() {
        dnsLookupThread.interrupt()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DNSServiceDiscovery::class.java)

        // Docker compose service name, used to lookup DNS.
        private val SERVICE_NAME = Environment.getServiceName()
    }
}
