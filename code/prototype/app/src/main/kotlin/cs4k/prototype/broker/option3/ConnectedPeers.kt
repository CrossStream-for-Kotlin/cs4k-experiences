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

class ConnectedPeers(
    private val selfInetAddress: InetAddress,
    private val sharedService: String,
    private val sharedPort: Int,
    private val dnsExpiresOn: Long = 5000
) {

    private val peers: MutableList<InetAddress> = mutableListOf()

    private val lock = ReentrantLock()

    init {
        dnsLookup()
    }

    private val dnsLookupThread = Thread {
        while (true) {
            sleep(dnsExpiresOn)
            logger.info("looking up dns...")
            dnsLookup()
        }
    }

    private fun dnsLookup() = lock.withLock {
        peers.clear()
        try {
            val ipAddresses = InetAddress.getAllByName(sharedService)
            for (ipAddress in ipAddresses) {
                when (ipAddress) {
                    is Inet4Address ->
                        if (ipAddress != selfInetAddress)
                            peers.add(ipAddress)
                }
            }
            logger.info("lookup dns, peers -> {}", peers.joinToString(" , "))
        } catch (ex: Exception) {
            logger.info("[ERROR]: {}", ex.stackTrace)
        }
    }

    fun send(socket: DatagramSocket, port: Int, buf: ByteArray) = lock.withLock {
        for (peer in peers) {
            val peerSocketAddress = InetSocketAddress(peer, port)
            val datagramPacket = DatagramPacket(buf, buf.size, peerSocketAddress)
            socket.send(datagramPacket)
        }
    }

    fun addIp(address: InetAddress) = lock.withLock {
        peers.add(address)
    }

    fun shutdown() = dnsLookupThread.interrupt()

    private companion object {
        private val logger = LoggerFactory.getLogger(ConnectedPeers::class.java)
    }

}