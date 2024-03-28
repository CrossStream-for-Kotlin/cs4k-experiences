package cs4k.prototype.broker

/**
 * Represent [Broker] exceptions.
 */
sealed class BrokerException : Exception() {

    /**
     * Cannot invoke broker methods because the broker is turned off.
     * Method [Broker.shutdown] already called.
     */
    class BrokerTurnOffException : BrokerException()
}
