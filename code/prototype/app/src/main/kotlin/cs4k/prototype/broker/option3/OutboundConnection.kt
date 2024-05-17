package cs4k.prototype.broker.option3

import java.net.InetSocketAddress
import java.nio.channels.AsynchronousSocketChannel

/**
 * Represents all information about outbound connection with a [Neighbor], i.e., the connection to send events.
 *
 * @property state The state of a connection.
 * @property inetSocketAddress The socket address.
 * @property socketChannel The socket channel to send events.
 */
data class OutboundConnection(
    val state: ConnectionState,
    val inetSocketAddress: InetSocketAddress,
    val socketChannel: AsynchronousSocketChannel? = null,
    var numberRetries: Int = 0
)
