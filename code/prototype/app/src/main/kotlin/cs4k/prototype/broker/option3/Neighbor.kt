package cs4k.prototype.broker.option3

import java.net.InetAddress

/**
 * Represents a neighbor on the network.
 *
 * @property inetAddress The IP address.
 * @property relationState The state of the relation with the neighbor.
 */
data class Neighbor(
    val inetAddress: InetAddress,
    val relationState: NeighborRelationState
)
