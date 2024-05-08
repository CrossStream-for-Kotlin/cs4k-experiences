package cs4k.prototype.broker.common

/**
 * Represents an event that can be published to a topic.
 *
 * @property topic The topic of the event.
 * @property id The identifier of the event.
 * @property message The message of the event.
 * @property isLast If the event is the last one.
 */
data class Event(
    val topic: String,
    val id: Long,
    val message: String,
    val isLast: Boolean = false
)
