package cs4k.prototype.broker.option3

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

@Component
class BrokerOption3 {

    private val inetAddress = InetAddress.getByName(MULTICAST_IP)
    private val inetSocketAddress = InetSocketAddress(inetAddress, MULTICAST_PORT)
    private val multicastSocket = MulticastSocket(MULTICAST_PORT)
    private val networkInterface = Utils.getActiveMulticastNetworkInterface()
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    private val associatedSubscribers = AssociatedSubscribers()

    init {
        multicastSocket.timeToLive = TIME_TO_LIVE
        multicastSocket.joinGroup(inetSocketAddress, networkInterface)

        thread {
            listenMulticastSocket()
        }
    }

    private fun listenMulticastSocket() {
        logger.info("start reading socket ip '{}' port '{}'", MULTICAST_IP, MULTICAST_PORT)
        while (!multicastSocket.isClosed) {
            try {
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

    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    fun publish(topic: String, message: String, isLastMessage: Boolean = false) {
        val event = Event(topic, -1, message, isLastMessage)
        val eventJsonBytes = BrokerSerializer.serializeEventToJson(event).toByteArray()

        val datagramPacket = DatagramPacket(eventJsonBytes, eventJsonBytes.size, inetSocketAddress)
        multicastSocket.send(datagramPacket)

        logger.info("publish topic '{}' event '{}", topic, event)
    }

    fun shutdown() {
        multicastSocket.leaveGroup(inetSocketAddress, networkInterface)
        multicastSocket.close()
        logger.info("broker turned off")
    }

    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(BrokerOption3::class.java)

        private const val MULTICAST_IP = "228.5.6.7"
        private const val MULTICAST_PORT = 6789
        private const val INBOUND_BUFFER_SIZE = 2048
        private const val TIME_TO_LIVE = 10
    }
}
