package cs4k.prototype.broker.option3

import java.net.InetAddress

/**
 * Represents a neighbor on the network.
 *
 * @property inetAddress The IP address.
 * @property inboundConnection All information about inbound connection, i.e., the connection to receive events.
 * @property inboundConnection The information about outbound connection, i.e, the connection to send events.
 */
data class Neighbor(
    val inetAddress: InetAddress,
    val inboundConnection: InboundConnection? = null,
    val outboundConnection: OutboundConnection? = null
) {

    val isOutboundConnectionActive
        get() = outboundConnection != null && outboundConnection.state == ConnectionState.CONNECTED

    val isInboundConnectionActive
        get() = inboundConnection != null && inboundConnection.state == ConnectionState.CONNECTED
}
