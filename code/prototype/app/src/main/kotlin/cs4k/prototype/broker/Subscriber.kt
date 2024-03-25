package cs4k.prototype.broker

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 * @param id the id of the subscriber.
 * @param handler the handler to call when an event is published to the topic.
 */
class Subscriber(
    val id: UUID,
    val handler: (event: Event) -> Unit
)
