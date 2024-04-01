package cs4k.prototype.broker

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 * @param id The id of the subscriber.
 * @param handler The handler to call when an event is published to the topic.
 */
data class Subscriber(
    val id: UUID,
    val handler: (event: Event) -> Unit
)
