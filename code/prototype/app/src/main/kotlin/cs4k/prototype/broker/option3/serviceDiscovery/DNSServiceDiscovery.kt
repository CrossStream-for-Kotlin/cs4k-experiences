package cs4k.prototype.broker.option3.serviceDiscovery

import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.option3.Neighbor
import cs4k.prototype.broker.option3.Neighbors
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.net.Inet4Address
import java.net.InetAddress
import kotlin.concurrent.thread

/**
 * Responsible for service discovery through Domain Name System (DNS) queries, i.e.:
 *  - Periodic querying the DNS server, giving it a name (that being a Docker service name).
 *  - Adding new entries to the neighbors.
 *
 * @property neighbors The set of neighbors.
 * @property selfInetAddress The node's own inet address (IP).
 * @property lookupAgainTime Amount of time, in milliseconds before another DNS query.
 */
class DNSServiceDiscovery(
    private val neighbors: Neighbors,
    private val selfInetAddress: InetAddress = InetAddress.getByName(Environment.getHostname()),
    private val lookupAgainTime: Long = DEFAULT_LOOKUP_AGAIN_TIME
) {

    // Retry executor.
    private val retryExecutor = RetryExecutor()

    // Thread responsible for making periodic DNS queries.
    private val dnsLookupThread = thread {
        retryExecutor.execute({ UnexpectedBrokerException() }, {
            try {
                while (true) {
                    logger.info("[{}] querying dns server", selfInetAddress.hostAddress)
                    dnsLookup()
                    sleep(lookupAgainTime)
                }
            } catch (ex: Exception) {
                if (ex is InterruptedException) {
                    logger.error("[{}] dns lookup interrupted", selfInetAddress.hostAddress)
                } else {
                    throw ex
                }
            }
        })
    }

    /**
     * Make a DNS query and add all new IP addresses received as neighbours.
     */
    private fun dnsLookup() {
        val neighborsIp = InetAddress.getAllByName(SERVICE_NAME)
            .filter { it is Inet4Address && it != selfInetAddress }
        val currentNeighbors = neighborsIp
            .map { Neighbor(it) }
            .toSet()
        neighbors.addAll(currentNeighbors)
        logger.info("[{}] dns lookup:: {}", selfInetAddress.hostAddress, neighborsIp.joinToString(" , "))
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

        private const val DEFAULT_LOOKUP_AGAIN_TIME = 5000L
    }
}
