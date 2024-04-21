package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * Container associating all topics with their respective consumers.
 */
class LatestTopicEvents {

    private data class LatestEvents(
        val received: Event?,
        val sent: Event?
    )

    // Map that associates topics with consumers
    private val map = HashMap<String, LatestEvents>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtain the latest received event.
     * @param topic The topic.
     * @return The latest received event.
     */
    fun getLatestReceivedEvent(topic: String) = lock.withLock {
        map[topic]?.received
    }

    /**
     * Obtain the latest sent event.
     * @param topic The topic.
     * @return The latest sent event.
     */
    fun getLatestSentEvent(topic: String) = lock.withLock {
        map[topic]?.sent
    }

    /**
     * Obtain the latest event id.
     * @param topic The topic.
     * @return The latest event id.
     */
    fun getNextEventId(topic: String) = lock.withLock {
        val events = map[topic] ?: return@withLock 0L
        max(
            events.received?.id?.plus(1) ?: 0L,
            events.sent?.id?.plus(1) ?: 0L
        )
    }

    fun setLatestReceivedEvent(topic: String, received: Event?) = lock.withLock {
        val events = map[topic]?.copy(received = received) ?: LatestEvents(received, null)
        setEvent(topic, events)
    }

    fun setLatestSentEvent(topic: String, sent: Event?) = lock.withLock {
        val events = map[topic]?.copy(sent = sent) ?: LatestEvents(null, sent)
        setEvent(topic, events)
    }

    /**
     * Define a consumer of a topic.
     * @param topic The topic.
     * @param events The latest received and sent events.
     *
     */
    private fun setEvent(topic: String, events: LatestEvents) = map.set(topic, events)
}
