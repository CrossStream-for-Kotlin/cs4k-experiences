package cs4k.prototype.broker

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 *
 * @property id The identifier of the subscriber.
 * @property lastEventNotified The last event id the subscriber was notified of. -1 is used to
 * say that the subscriber has not received any messages.
 * @property handler The handler to call when an event is published to the topic.
 */
data class Subscriber(
    val id: UUID,
    val lastEventNotified: Long,
    val handler: (event: Event) -> Unit
) {

    constructor(id: UUID, handler: (event: Event) -> Unit) : this(id, -1, handler)
}
