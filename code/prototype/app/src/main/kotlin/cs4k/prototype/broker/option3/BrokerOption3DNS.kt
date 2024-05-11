package cs4k.prototype.broker.option3

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketException
import java.util.UUID
import kotlin.concurrent.thread

@Component
class BrokerOption3DNS : Broker {

    private val inetAddress = InetAddress.getByName(Environment.getHost())

    private val inboundInetSocketAddress = InetSocketAddress(inetAddress, INBOUND_PORT)
    private val inboundSocket = DatagramSocket(inboundInetSocketAddress)
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    private val outboundInetSocketAddress = InetSocketAddress(inetAddress, OUTBOUND_PORT)
    private val outboundSocket = DatagramSocket(outboundInetSocketAddress)

    private val checkInInetSocketAddress = InetSocketAddress(inetAddress, CHECKIN_PORT)
    private val checkInSocket = DatagramSocket(checkInInetSocketAddress)

    private val associatedSubscribers = AssociatedSubscribers()
    private val connectedPeers = ConnectedPeers(inetAddress, SERVICE_NAME, INBOUND_PORT)

    init {
        thread {
            listenSocket()
        }
        connectedPeers.send(checkInSocket, CHECKIN_PORT, "new node".toByteArray())
        thread {
            listenNewBrokers()
        }
    }

    private fun listenSocket() {
        logger.info("events: start reading socket ip '{}' port '{}'", inboundSocket.localAddress, INBOUND_PORT)
        while (!inboundSocket.isClosed) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                inboundSocket.receive(receivedDatagramPacket)
                val receivedMessage = String(receivedDatagramPacket.data.copyOfRange(0, receivedDatagramPacket.length))
                val event = BrokerSerializer.deserializeEventFromJson(receivedMessage)
                logger.info("new event topic '{}' event '{}", event.topic, event)
                associatedSubscribers
                    .getAll(event.topic)
                    .forEach { subscriber -> subscriber.handler(event) }
            } catch (e: SocketException) {
                logger.info("[ERROR]: {}", e.stackTrace)
                inboundSocket.close()
                break
            }
        }
    }

    private fun listenNewBrokers() {
        logger.info("new brokers: start reading socket ip '{}' port '{}'", checkInSocket.localAddress, CHECKIN_PORT)
        while (!checkInSocket.isClosed) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                inboundSocket.receive(receivedDatagramPacket)
                logger.info("new broker detected")
                connectedPeers.dnsLookup()
            } catch (e: SocketException) {
                logger.info("[ERROR]: {}", e.stackTrace)
                checkInSocket.close()
                break
            }
        }
    }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        val event = Event(topic, 0, message, isLastMessage)
        val eventJsonBytes = BrokerSerializer.serializeEventToJson(event).toByteArray()

        connectedPeers.send(outboundSocket, INBOUND_PORT, eventJsonBytes)

        logger.info("publish topic '{}' event '{}", topic, event)
    }

    override fun shutdown() {
        connectedPeers.shutdown()
        outboundSocket.close()
        inboundSocket.close()
        logger.info("broker turned off")
    }

    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(BrokerOption3DNS::class.java)

        private val SERVICE_NAME = Environment.getServiceName()

        private const val INBOUND_PORT = 6789
        private const val OUTBOUND_PORT = 6790
        private const val CHECKIN_PORT = 6791

        private const val INBOUND_BUFFER_SIZE = 2048
    }
}
