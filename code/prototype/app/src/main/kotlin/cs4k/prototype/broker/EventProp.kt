package cs4k.prototype.broker

enum class EventProp {
    ID, MESSAGE, IS_LAST;

    override fun toString() = name.lowercase()
}