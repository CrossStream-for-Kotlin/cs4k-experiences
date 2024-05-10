package cs4k.prototype.broker.option3

import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.Subscriber
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.DatagramPacket
import java.net.InetAddress
import java.net.MulticastSocket
import java.net.SocketException
import java.util.UUID
import kotlin.concurrent.thread

@Component
class BrokerOption3 {

    private val multicastGroup: InetAddress = InetAddress.getByName(MULTICAST_IP)
    private val socket = MulticastSocket(MULTICAST_PORT)
    private val inboundBuffer = ByteArray(INBOUND_BUFFER_SIZE)

    private val associatedSubscribers = AssociatedSubscribers()

    init {
        socket.joinGroup(multicastGroup)

        thread {
            listenSocket()
        }
    }

    fun listenSocket() {
        while (true) {
            try {
                val receivedDatagramPacket = DatagramPacket(inboundBuffer, inboundBuffer.size)
                socket.receive(receivedDatagramPacket)

                val receivedMessage = String(receivedDatagramPacket.data.copyOfRange(0, receivedDatagramPacket.length))
                val receivedEvent = BrokerSerializer.deserializeEventFromJson(receivedMessage)
                logger.info("new event topic '{}' event '{}", receivedEvent.topic, receivedEvent)
                associatedSubscribers
                    .getAll(receivedEvent.topic)
                    .forEach { subscriber -> subscriber.handler(receivedEvent) }
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
        val eventJson = BrokerSerializer.serializeEventToJson(Event(topic, -1, message, isLastMessage))
        logger.info("publish topic '{}' event '{}", topic, eventJson)

        val datagramPacket = DatagramPacket(eventJson.toByteArray(), eventJson.length, multicastGroup, MULTICAST_PORT)
        socket.send(datagramPacket)
    }

    fun shutdown() {
        socket.leaveGroup(multicastGroup)
        socket.close()
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
    }
}
