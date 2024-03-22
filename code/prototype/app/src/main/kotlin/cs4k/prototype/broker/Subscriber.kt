package cs4k.prototype.broker

class Subscriber(
    val topic: String,
    val handler: (event: Event) -> Unit
)
