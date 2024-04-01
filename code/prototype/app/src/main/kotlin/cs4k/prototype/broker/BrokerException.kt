package cs4k.prototype.broker

/**
 * Represent [Broker] exceptions.
 */
sealed class BrokerException(msg: String="") : Exception(msg) {


    /**
     * Exception indicating that the connection to the database couldn't be established.
     */
    class BrokerDbConnectionException : BrokerException("Connection to the database could not be established")

    /**
     * Exception indicating that the broker lost connection to the database.
     */
    class BrokerDbLostConnectionException : BrokerException("Lost connection to the database")

    /**
     * Exception indicating that the broker is turned off.
     * @param msg The message to show.
     */
    class BrokerTurnOffException(msg: String =" ") : BrokerException(msg)

    /**
     * Missing environment variable required by broker.
     */
    class EnvironmentVariableException(msg: String) : BrokerException(msg)

    /**
     * Something unexpected happened at the broker.
     */
    class UnexpectedBrokerException(msg: String = UNEXPECTED_BROKER_EXCEPTION_DEFAULT_MESSAGE) : BrokerException(msg)

    companion object {
        const val UNEXPECTED_BROKER_EXCEPTION_DEFAULT_MESSAGE = "Something unexpected happened, try again later."
    }
}
