package cs4k.prototype.broker.common

/**
 * Represent Broker exceptions.
 *
 * @param msg The message to show.
 */
sealed class BrokerException(msg: String) : Exception(msg) {

    /**
     * Exception indicating that the connection to the external system couldn't be established.
     */
    class BrokerConnectionException : BrokerException(BROKER_CONNECTION_EXCEPTION_DEFAULT_MESSAGE)

    /**
     * Exception indicating that the broker lost connection to the external system.
     */
    class BrokerLostConnectionException : BrokerException(BROKER_LOST_CONNECTION_EXCEPTION_DEFAULT_MESSAGE)

    /**
     * Exception indicating that the broker is turned off.
     *
     * @param msg The message to show.
     */
    class BrokerTurnOffException(msg: String) : BrokerException(msg)

    /**
     * Missing environment variable required by broker.
     *
     * @param msg The message to show.
     */
    class EnvironmentVariableException(msg: String) : BrokerException(msg)

    /**
     * Connection poll size not allowed.
     *
     * @param msg The message to show.
     */
    class ConnectionPoolSizeException(msg: String) : BrokerException(msg)

    /**
     * Something unexpected happened at the broker.
     *
     * @param msg The message to show.
     */
    class UnexpectedBrokerException(msg: String = UNEXPECTED_BROKER_EXCEPTION_DEFAULT_MESSAGE) : BrokerException(msg)

    companion object {
        const val UNEXPECTED_BROKER_EXCEPTION_DEFAULT_MESSAGE =
            "Something unexpected happened, try again later."

        const val BROKER_CONNECTION_EXCEPTION_DEFAULT_MESSAGE =
            "Connection to the external system could not be established."

        const val BROKER_LOST_CONNECTION_EXCEPTION_DEFAULT_MESSAGE =
            "Lost connection to the external system."
    }
}