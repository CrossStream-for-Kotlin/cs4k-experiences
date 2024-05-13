package cs4k.prototype.broker.option3

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.SocketException
import java.util.UUID
import kotlin.concurrent.thread

// @Component
class BrokerOption3Multicast : Broker {

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

    // Association between topics and subscribers lists.
    private val associatedSubscribers = AssociatedSubscribers()

    init {
        // Redefine the Time To Live value of IP multicast packets sent.
        // I.e. The maximum number of machine-to-machine hops that packets can make before being discarded.
        multicastSocket.timeToLive = TIME_TO_LIVE

        // Join the multicast group.
        multicastSocket.joinGroup(inetSocketAddress, networkInterface)

        // Start a new thread ...
        thread {
            // to listen for IP multicast packets.
            listenMulticastSocket()
        }
    }

    /**
     * Listen for IP multicast packets.
     */
    private fun listenMulticastSocket() {
        logger.info("start reading socket ip '{}' port '{}'", MULTICAST_IP, MULTICAST_PORT)
        while (!multicastSocket.isClosed) {
            try {
                // Receive a datagram packet via socket.
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                multicastSocket.receive(receivedDatagramPacket)

                val receivedMessage = String(receivedDatagramPacket.data.copyOfRange(0, receivedDatagramPacket.length))
                val event = BrokerSerializer.deserializeEventFromJson(receivedMessage)
                logger.info("new event topic '{}' event '{}", event.topic, event)
                associatedSubscribers
                    .getAll(event.topic)
                    .forEach { subscriber -> subscriber.handler(event) }
            } catch (e: SocketException) {
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
        val event = Event(topic, -1, message, isLastMessage)
        val eventJsonBytes = BrokerSerializer.serializeEventToJson(event).toByteArray()

        // Send a datagram packet via socket.
        val datagramPacket = DatagramPacket(eventJsonBytes, eventJsonBytes.size, inetSocketAddress)
        multicastSocket.send(datagramPacket)

        logger.info("publish topic '{}' event '{}", topic, event)
    }

    override fun shutdown() {
        // Leave the multicast group.
        multicastSocket.leaveGroup(inetSocketAddress, networkInterface)
        multicastSocket.close()
        logger.info("broker turned off")
    }

    /**
     * Unsubscribe from a topic.
     *
     * @param topic The topic name.
     * @param subscriber The subscriber who unsubscribed.
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(BrokerOption3Multicast::class.java)

        private const val MULTICAST_IP = "228.5.6.7"
        private const val MULTICAST_PORT = 6789
        private const val INBOUND_BUFFER_SIZE = 2048
        private const val TIME_TO_LIVE = 10
    }
}
