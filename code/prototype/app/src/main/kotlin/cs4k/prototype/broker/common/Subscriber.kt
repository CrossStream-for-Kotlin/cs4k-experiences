package cs4k.prototype.broker.common

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 *
 * @property id The identifier of the subscriber.
 * @property handler The handler to call when an event is published to the topic.
 * @property lastEventId The identifier of the last event the subscriber received. '-1' is used to indicate that
 * the subscriber has not received any event.
 */
data class Subscriber(
    val id: UUID,
    val handler: (event: Event) -> Unit,
    val lastEventId: Long = -1
)
