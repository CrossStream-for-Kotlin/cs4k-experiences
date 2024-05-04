package cs4k.prototype.broker.option2.redis

import cs4k.prototype.broker.common.Event

/**
 * Represents the [Event] properties.
 */
enum class EventProp {
    TOPIC, ID, MESSAGE, IS_LAST;

    override fun toString() = name.lowercase()
}
