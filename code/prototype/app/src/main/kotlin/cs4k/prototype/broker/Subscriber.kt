package cs4k.prototype.broker

import java.util.UUID

class Subscriber(
    val id: UUID,
    val handler: (event: Event) -> Unit
)
