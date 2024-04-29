package cs4k.prototype.broker

/**
 * Represents the [Event] properties.
 */
enum class EventProp {
    TOPIC, ID, MESSAGE, IS_LAST;

    override fun toString() = name.lowercase()
}