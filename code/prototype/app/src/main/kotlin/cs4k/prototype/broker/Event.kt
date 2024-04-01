package cs4k.prototype.broker

/**
 * Represents an event that can be published to a topic.
 * @param topic The topic of the event.
 * @param id The id of the event.
 * @param message The message of the event.
 * @param isLast If the event is the last one.
 */
data class Event(
    val topic: String,
    val id: Long,
    val message: String,
    val isLast: Boolean = false
)
