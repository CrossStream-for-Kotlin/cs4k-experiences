package cs4k.prototype.broker

/**
 * Represent [Broker] exceptions.
 */
sealed class BrokerException(msg: String) : Exception(msg) {

    /**
     * Cannot invoke broker methods because the broker is turned off.
     * Method [Broker.shutdown] already called.
     */
    class BrokerTurnOffException(msg: String) : BrokerException(msg)

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
