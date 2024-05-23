package cs4k.prototype.broker.common

import java.util.UUID

/**
 * Represents a subscriber to a topic.
 *
 * @property id The identifier of the subscriber.
 * @property handler The handler to call when an event is published to the topic.
 * @property lastEventIdReceived The identifier of the last event the subscriber received.
 * '-1' is used to indicate that the subscriber has not received any event.
 */
data class SubscriberWithEventTracking(
    override val id: UUID,
    override val handler: (event: Event) -> Unit,
    val lastEventIdReceived: Long = -1
) : BaseSubscriber
