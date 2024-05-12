package cs4k.prototype.broker

import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.BrokerException.UnexpectedBrokerException
import cs4k.prototype.broker.common.Event

/**
 * Public contract of Broker.
 */
interface Broker {

    /**
     * Subscribe to a topic.
     *
     * @param topic The topic name.
     * @param handler The handler to be called when there is a new event.
     * @return The method to be called when unsubscribing.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    fun subscribe(topic: String, handler: (event: Event) -> Unit): () -> Unit

    /**
     * Publish a message to a topic.
     *
     * @param topic The topic name.
     * @param message The message to send.
     * @param isLastMessage Indicates if the message is the last one.
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     * @throws UnexpectedBrokerException If something unexpected happens.
     */
    fun publish(topic: String, message: String, isLastMessage: Boolean = false)

    /**
     * Shutdown the broker.
     *
     * @throws BrokerTurnOffException If the broker is turned off.
     * @throws BrokerLostConnectionException If the broker lost connection to the external system.
     */
    fun shutdown()
}
