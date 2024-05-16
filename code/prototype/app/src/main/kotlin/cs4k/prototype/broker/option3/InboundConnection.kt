package cs4k.prototype.broker.option3

import java.nio.channels.AsynchronousSocketChannel

/**
 * Represents all information about inbound connection with a [Neighbor], i.e., the connection to receive events.
 *
 * @property state The state of a connection.
 * @property socketChannel The socket channel to receive events.
 */
data class InboundConnection(
    val state: ConnectionState,
    val socketChannel: AsynchronousSocketChannel
)
