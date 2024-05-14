package cs4k.prototype.broker.option3

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class AssociatedPeers {

    val set = hashSetOf<Peer>()

    val lock = ReentrantLock()

    fun addPeer(peer: Peer) {
        lock.withLock {
            if (!set.contains(peer)) set.add(peer)
        }
    }
}
