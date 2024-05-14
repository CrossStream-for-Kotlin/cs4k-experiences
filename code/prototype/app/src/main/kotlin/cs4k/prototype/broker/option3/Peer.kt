package cs4k.prototype.broker.option3

import java.net.InetAddress

data class Peer(
    val inetAddress: InetAddress,
    val state: PeerState
)
