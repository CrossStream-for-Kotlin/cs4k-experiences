package cs4k.prototype.broker.option3

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.option3.ConnectionState.CONNECTED
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.Executors
import kotlin.concurrent.thread

@Component
class BrokerOption3 : Broker {

    private val associatedSubscribers = AssociatedSubscribers()

    private val neighbors = Neighbors()

    private val selfIp = InetAddress.getByName(Environment.getHost()).hostAddress

    private val serverSocketChannel = AsynchronousServerSocketChannel.open()

    // private val eventsToSend = MessageQueue<Event>(EVENTS_TO_SEND_CAPACITY)

    init {
        // DNSServiceDiscovery(neighbors)
        MulticastServiceDiscovery(neighbors, selfIp)

        thread {
            runBlocking {
                val listenServerSocketChannelJob = this.launch(readCoroutineDispatcher) {
                    listenServerSocketChannel(this)
                }

                val periodicConnectToNeighboursJob = this.launch(writeCoroutineDispatcher) {
                    periodicConnectToNeighbours()
                }

                // val processEventsToSendJob = this.launch(writeDispatcher) {
                //    processEventsToSend()
                // }

                listenServerSocketChannelJob.join()
                periodicConnectToNeighboursJob.join()
                // processEventsToSendJob.join()
            }
        }
    }

    private suspend fun periodicConnectToNeighbours() {
        while (true) {
            neighbors
                .getAll()
                .forEach { neighbor ->
                    if (!neighbor.isOutboundConnectionActive) {
                        logger.info("[NODE IP '{}'] try connect to node ip '{}", selfIp, neighbor.inetAddress)
                        val inetOutboundSocketAddress = InetSocketAddress(neighbor.inetAddress, COMMON_PORT)
                        try {
                            val outboundSocketChannel = AsynchronousSocketChannel.open()
                            outboundSocketChannel.connectSuspend(inetOutboundSocketAddress)

                            neighbors.update(
                                neighbor.copy(
                                    outboundConnection = OutboundConnection(
                                        state = CONNECTED,
                                        inetSocketAddress = inetOutboundSocketAddress,
                                        socketChannel = outboundSocketChannel
                                    )
                                )
                            )

                            logger.info(
                                "[NODE IP '{}'] establish an outbound connection with node ip '{}' ",
                                selfIp,
                                inetOutboundSocketAddress
                            )
                        } catch (ex: ConnectException) {
                            logger.info(
                                "[NODE IP '{}'] can not establish an outbound connection with node ip '{}' ",
                                selfIp,
                                inetOutboundSocketAddress
                            )
                        }
                    }
                }
            delay(DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN)
        }
    }

    private suspend fun listenServerSocketChannel(coroutineScope: CoroutineScope) {
        serverSocketChannel.use {
            serverSocketChannel.bind(InetSocketAddress(selfIp, COMMON_PORT))
            logger.info("[NODE IP '{}'] server socket bound", selfIp)
            while (true) {
                val inboundSocketChannel = serverSocketChannel.acceptSuspend()
                coroutineScope.launch(readCoroutineDispatcher) {
                    val inboundConnection = InboundConnection(
                        state = CONNECTED,
                        socketChannel = inboundSocketChannel
                    )
                    val inetAddress = (inboundSocketChannel.remoteAddress as InetSocketAddress).address
                    val neighbor = neighbors.get(inetAddress) ?: Neighbor(inetAddress)
                    val updatedNeighbor = neighbors.update(neighbor.copy(inboundConnection = inboundConnection))
                    logger.info(
                        "[NODE IP '{}'] establish an inbound connection with node ip '{}' ",
                        selfIp,
                        inetAddress
                    )
                    listenSocketChannel(requireNotNull(updatedNeighbor))
                }
            }
        }
    }

    private suspend fun listenSocketChannel(neighbor: Neighbor) {
        val byteBuffer = ByteBuffer.allocate(INBOUND_BUFFER_SIZE)
        while (true) {
            if (neighbor.isInboundConnectionActive) {
                val readLength = neighbor.inboundConnection?.socketChannel?.readSuspend(byteBuffer)
                val event = BrokerSerializer.deserializeEventFromJson(
                    String(byteBuffer.reset().array(), 0, requireNotNull(readLength))
                )
                deliverToSubscribers(event)
                logger.info(
                    "[NODE IP '{}'] receive event '{}' from node ip '{}' ",
                    selfIp,
                    event,
                    neighbor.inetAddress
                )
            }
        }
    }

    private fun deliverToSubscribers(event: Event) {
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    // private suspend fun processEventsToSend() {
    //    while (true) {
    //        val event = eventsToSend.dequeue(Duration.INFINITE)
    //        logger.info("[NODE '{}'] process event '{}' ", selfIp, event)
    //        deliverToSubscribers(event)
    //
    //        val eventJson = BrokerSerializer.serializeEventToJson(event)
    //        sendToNeighbors(eventJson)
    //    }
    // }

    // private suspend fun sendToNeighbors(eventJson: String) {
    //    neighbors
    //        .getAll()
    //        .forEach { neighbor ->
    //            if (neighbor.isOutboundConnectionActive) {
    //                neighbor.outboundConnection?.socketChannel?.writeSuspend(eventJson)
    //                logger.info(
    //                    "[NODE '{}'] send event '{}' to node ip '{}' ",
    //                    selfIp,
    //                    eventJson,
    //                    neighbor.inetAddress
    //                )
    //            }
    //        }
    // }

    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        TODO("Not yet implemented")
        //    val subscriber = Subscriber(UUID.randomUUID(), handler)
        //    associatedSubscribers.addToKey(topic, subscriber)
        //
        //    logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)
        //
        //    return { unsubscribe(topic, subscriber) }
    }

    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        TODO("Not yet implemented")
        //    val event = Event(topic, IGNORE_EVENT_ID, message, isLastMessage)
        //    runBlocking {
        //        eventsToSend.enqueue(event)
        //    }
        //
        //    logger.info("publish topic '{}' event '{}", topic, event)
    }

    override fun shutdown() {
        TODO("Not yet implemented")
    }

    // private fun unsubscribe(topic: String, subscriber: Subscriber) {
    //     associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
    //    logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    // }

    private companion object {
        private val logger = LoggerFactory.getLogger(BrokerOption3::class.java)

        private val readCoroutineDispatcher =
            Executors.newFixedThreadPool(3).asCoroutineDispatcher()
        private val writeCoroutineDispatcher =
            Executors.newFixedThreadPool(3).asCoroutineDispatcher()

        private const val IGNORE_EVENT_ID = -1L
        private const val COMMON_PORT = 6790
        private const val INBOUND_BUFFER_SIZE = 2048
        private const val EVENTS_TO_SEND_CAPACITY = 5000
        private const val DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN = 2000L
    }
}
