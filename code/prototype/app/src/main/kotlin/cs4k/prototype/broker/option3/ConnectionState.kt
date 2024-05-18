package cs4k.prototype.broker.option3

/**
 * Represents the state of a connection with a [Neighbor].
 *
 * - 'CONNECTED': Active connection with neighbor.
 * - 'DISCONNECTED': Disconnect connection with neighbor.
 * - 'ZOMBIE': Something is wrong with the connection with the neighbor, but the neighbor exists.
 */
enum class ConnectionState {
    CONNECTED, DISCONNECTED, ZOMBIE
}
