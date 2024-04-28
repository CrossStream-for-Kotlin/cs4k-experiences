package cs4k.prototype.broker

enum class EventProp {
    TOPIC, ID, MESSAGE, IS_LAST;

    override fun toString() = name.lowercase()
}