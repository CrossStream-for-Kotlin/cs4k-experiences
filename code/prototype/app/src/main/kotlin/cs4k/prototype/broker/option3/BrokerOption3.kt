package cs4k.prototype.broker.option3

import cs4k.prototype.broker.Broker
import cs4k.prototype.broker.common.AssociatedSubscribers
import cs4k.prototype.broker.common.BrokerException
import cs4k.prototype.broker.common.BrokerException.*
import cs4k.prototype.broker.common.BrokerSerializer
import cs4k.prototype.broker.common.Environment
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.common.RetryExecutor
import cs4k.prototype.broker.common.Subscriber
import cs4k.prototype.broker.option3.ConnectionState.CONNECTED
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.ConnectException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.concurrent.thread
import kotlin.time.Duration

@Component
class BrokerOption3 : Broker {

    // The set of subscribers associated with topics.
    private val associatedSubscribers = AssociatedSubscribers()

    // The set of neighbors that the broker knows.
    private val neighbors = Neighbors()

    // The IP address of the node.
    private val selfIp = InetAddress.getByName(Environment.getHostname()).hostAddress

    // The server socket channel to accept inbound connections from neighbors.
    private val serviceDiscovery = MulticastServiceDiscovery(neighbors, selfIp)
    // DNSServiceDiscovery(neighbors)

    private val serverSocketChannel = AsynchronousServerSocketChannel.open()

    // The queue of events to send to neighbors.
    private val eventsToSend = MessageQueue<Event>(EVENTS_TO_SEND_CAPACITY)

    private val retryExecutor = RetryExecutor()

    private var scope: CoroutineScope? = null

    init {
        thread {
            runBlocking {
                scope = this

                supervisorScope {
                    // Start the server socket channel to accept inbound connections.
                    val listenServerSocketChannelJob = this.launch(readCoroutineDispatcher) {
                        listenServerSocketChannel(this)
                    }
                    // Periodically connect new neighbors.
                    val periodicConnectToNeighboursJob = this.launch(writeCoroutineDispatcher) {
                        periodicConnectToNeighbours()
                    }
                    val processEventsToSendJob = this.launch(writeCoroutineDispatcher) {
                        processEventsToSend()
                    }

                    joinAll(listenServerSocketChannelJob, periodicConnectToNeighboursJob, processEventsToSendJob)
                }
            }
        }
    }

    /**
     * Periodically refreshing the outBound connections to neighbors.
     * In case of having a new neighbor, try to connect to it.
     * If some reason the connection is lost, try to reconnect.
     */
    private suspend fun periodicConnectToNeighbours() {
        while (true) {
            neighbors
                .getAll()
                .forEach { neighbor ->
                    if (!neighbor.isOutboundConnectionActive) {
                        logger.info("[NODE IP '{}'] try connect to node ip '{}", selfIp, neighbor.inetAddress)
                        val inetOutboundSocketAddress = InetSocketAddress(neighbor.inetAddress, COMMON_PORT)

                            retryExecutor.suspendExecute(
                                exception = { BrokerConnectionException().also {
                                    neighbors.updateAndGet(neighbor.copy(outboundConnection = neighbor.outboundConnection?.let {
                                        it.copy(numberRetries = it.numberRetries + 1)
                                        it.copy(state = ConnectionState.NOT_CONNECTED)
                                    }))
                                }},
                                action = {
                                    logger.info("[NODE IP '{}'] try connect to node ip '{}", selfIp, neighbor.inetAddress)

                                    // Establish an outbound connection with the neighbor.
                                    val outboundSocketChannel = AsynchronousSocketChannel.open()
                                    outboundSocketChannel.connectSuspend(inetOutboundSocketAddress)

                                    // Update the neighbor with the outbound connection, with the corresponding state and socket channel.
                                    neighbors.updateAndGet(
                                        neighbor.copy(
                                            outboundConnection = OutboundConnection(
                                                state = CONNECTED,
                                                inetSocketAddress = inetOutboundSocketAddress,
                                                socketChannel = outboundSocketChannel,
                                                numberRetries = 0
                                            )
                                        )
                                    )

                                    logger.info(
                                        "[NODE IP '{}'] establish an outbound connection with node ip '{}' ",
                                        selfIp,
                                        inetOutboundSocketAddress
                                    )
                                },
                                retryCondition = { ex ->
                                    if (ex is ConnectException) {
                                        logger.info(
                                            "[NODE IP '{}'] can not establish an outbound connection with node ip '{}' ",
                                            selfIp,
                                            inetOutboundSocketAddress
                                        )
                                        true
                                    } else {
                                        false
                                    }
                                }
                            )
                    }
                }
            delay(DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN)
        }
    }

    /**
     * Listen for inbound connections. Establishes the ServerSocketChannel and listens for inbound connections.
     * @param coroutineScope the coroutine scope
     * @return the job
     */
    private suspend fun listenServerSocketChannel(coroutineScope: CoroutineScope) {
        serverSocketChannel.use {
            // Bind the server socket channel to the IP address and port of the broker.
            serverSocketChannel.bind(InetSocketAddress(selfIp, COMMON_PORT))
            logger.info("[NODE IP '{}'] server socket bound", selfIp)
            while (true) {
                // Accept inbound connections from neighbors.
                val inboundSocketChannel = serverSocketChannel.acceptSuspend()
                coroutineScope.launch(readCoroutineDispatcher) {
                    // Establish an inbound connection with the neighbor.
                    val inboundConnection = InboundConnection(
                        state = CONNECTED,
                        socketChannel = inboundSocketChannel
                    )
                    // Get the IP address of the neighbor.
                    val inetAddress = (inboundSocketChannel.remoteAddress as InetSocketAddress).address
                    // Update the neighbor with the inbound connection.

                    val neighbor = neighbors.getBy(inetAddress) ?: Neighbor(inetAddress)
                    val updatedNeighbor = neighbors.updateAndGet(neighbor.copy(inboundConnection = inboundConnection))
                    logger.info(
                        "[NODE IP '{}'] establish an inbound connection with node ip '{}' ",
                        selfIp,
                        inetAddress
                    )
                    // Listen to the socket channel.
                    listenSocketChannel(requireNotNull(updatedNeighbor))
                }
            }
        }
    }

    /**
     * Listen to the socket channel. Reads the socket channel and processes the events received.
     * @param neighbor the neighbor
     */
    private suspend fun listenSocketChannel(neighbor: Neighbor) {
        while (true) {
            // If the inbound connection is active, read the socket channel.
            if (neighbor.isInboundConnectionActive) {
                // Read the socket channel.
                val lineReader = LineReader { requireNotNull(neighbor.inboundConnection).socketChannel.readSuspend(it) }
                lineReader.readLine()?.let { line ->
                    // Deserialize the event from JSON.
                    val event = BrokerSerializer.deserializeEventFromJson(line)
                    sendEventToSubscribers(event)
                    logger.info(
                        "[NODE IP '{}'] receive event '{}' from node ip '{}' ",
                        selfIp,
                        event,
                        neighbor.inetAddress
                    )
                }
            }
        }
    }

    /**
     * Send the event to the subscribers.
     */
    private fun sendEventToSubscribers(event: Event) {
        associatedSubscribers
            .getAll(event.topic)
            .forEach { subscriber -> subscriber.handler(event) }
    }

    /**
     * Process the events to send. Sends the events to the subscribers and neighbors.
     */
    private suspend fun processEventsToSend() {
        while (true) {
            val event = eventsToSend.dequeue(Duration.INFINITE)
            logger.info("[NODE '{}'] process event '{}' ", selfIp, event)
            // Send the event to the subscribers.
            sendEventToSubscribers(event)

            val eventJson = BrokerSerializer.serializeEventToJson(event)
            // Send the event to the neighbors.
            sendToNeighbors(eventJson)
        }
    }

    /**
     * Send the event to the neighbors.
     */
    private suspend fun sendToNeighbors(eventJson: String) {
        neighbors
            .getAll()
            .forEach { neighbor ->
                // If the outbound connection is active, write the event to the socket channel.
                if (neighbor.isOutboundConnectionActive) {
                    // Write the event to the socket channel.
                    neighbor.outboundConnection?.socketChannel?.writeSuspend(eventJson)
                    logger.info(
                        "[NODE IP '{}'] send event '{}' to node ip '{}' ",
                        selfIp,
                        eventJson,
                        neighbor.inetAddress
                    )
                }
            }
    }

    /**
     * Subscribe to a topic.
     * @param topic the topic
     * @param handler the handler
     * @return the unsubscribe function
     * @see Broker.subscribe
     */
    override fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit {
        val subscriber = Subscriber(UUID.randomUUID(), handler)
        associatedSubscribers.addToKey(topic, subscriber)

        logger.info("new subscriber topic '{}' id '{}", topic, subscriber.id)

        return { unsubscribe(topic, subscriber) }
    }

    /**
     * Publish a message to a topic.
     * @param topic the topic
     * @param message the message
     * @param isLastMessage the flag to indicate if it is the last message
     * @see Broker.publish
     */
    override fun publish(topic: String, message: String, isLastMessage: Boolean) {
        val event = Event(topic, IGNORE_EVENT_ID, message, isLastMessage)
        runBlocking {
            // Enqueue the event to send.
            eventsToSend.enqueue(event)
        }

        logger.info("publish topic '{}' event '{}", topic, event)
    }

    override fun shutdown() {
        serviceDiscovery.stop()
        scope?.cancel()
    }

    /**
     * Unsubscribe from a topic.
     * @param topic the topic
     * @param subscriber the subscriber
     * @see Broker.unsubscribe
     */
    private fun unsubscribe(topic: String, subscriber: Subscriber) {
        associatedSubscribers.removeIf(topic, { sub -> sub.id == subscriber.id })
        logger.info("unsubscribe topic '{}' id '{}", topic, subscriber.id)
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(BrokerOption3::class.java)

        private val readCoroutineDispatcher =
            Executors.newFixedThreadPool(3).asCoroutineDispatcher()
        private val writeCoroutineDispatcher =
            Executors.newFixedThreadPool(3).asCoroutineDispatcher()

        private const val IGNORE_EVENT_ID = -1L
        private const val COMMON_PORT = 6790
        private const val EVENTS_TO_SEND_CAPACITY = 5000
        private const val DEFAULT_WAIT_TIME_TO_REFRESH_NEIGHBOURS_AGAIN = 2000L
    }
}
