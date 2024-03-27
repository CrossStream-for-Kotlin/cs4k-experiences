package cs4k.prototype.broker

import kotlinx.serialization.Serializable

/**
 * Represents an event that can be published to a topic.
 * @param topic the topic of the event.
 * @param id the id of the event.
 * @param message the message of the event.
 * @param isLast if the event is the last one.
 */
@Serializable
data class Event(
    val topic: String,
    val id: Long,
    val message: String,
    val isLast: Boolean = false
)
