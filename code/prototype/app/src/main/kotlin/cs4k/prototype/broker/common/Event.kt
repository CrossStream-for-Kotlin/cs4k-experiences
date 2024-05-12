package cs4k.prototype.broker.common

import java.sql.Timestamp

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
    val isLast: Boolean = false,
    val timestamp: Timestamp = Timestamp(System.currentTimeMillis())
) {
    /**
     * Represents the events properties.
     *
     * @property key The name of the key associated with the event property.
     */
    enum class Prop(val key: String) {
        TOPIC("topic"),
        ID("id"),
        MESSAGE("message"),
        IS_LAST("is_last");
    }
}
