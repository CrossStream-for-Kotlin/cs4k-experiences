package cs4k.prototype.broker.option2.rabbitmq

import cs4k.prototype.broker.common.Event
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max

/**
 * Container associating all topics with their latest events.
 */
class LatestTopicEvents {

    /**
     * Represents the latest events of a given topic.
     * @param received The latest received message.
     * @param sent The latest sent message.
     */
    private data class LatestEvents(
        val received: EventInfo?,
        val sent: EventInfo?
    )

    /**
     * Represents the information regarding an event.
     * @param id Sequential event ID.
     * @param payload The message of the event joined with the indicator if the message is the last one of the topic.
     */
    private class EventInfo(
        val id: Long,
        val payload: String
    ) {

        /**
         * Converting an event to the information of the event to reduce memory.
         * @param event The complete event.
         */
        constructor(event: Event) :
            this(event.id, listOf(event.message, event.isLast.toString()).joinToString(";"))

        /**
         * Converting the information of the event into an actual event to notify a subscriber.
         * @param topic The topic of the event.
         * @return The actual event.
         */
        fun toEvent(topic: String): Event {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            return Event(topic, id, message, isLast)
        }
    }

    // Map that associates topics with their latest events.
    private val map = HashMap<String, LatestEvents>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    // Map that associates topics with all events.
    private val mapAllEventsFromTopic = HashMap<String, MutableList<Event>>()

    /**
     * Obtain the latest topic. When both sent and received are defined, received is prioritized.
     * @param topic The topic of the event desired.
     * @return The latest event of the topic.
     */
    fun getLatestEvent(topic: String): Event? = lock.withLock {
        val events = map[topic]
        return events?.received?.toEvent(topic) ?: events?.sent?.toEvent(topic)
    }

    /**
     * Obtain the latest received event.
     * @param topic The topic.
     * @return The latest received event.
     */
    fun getLatestReceivedEvent(topic: String) = lock.withLock {
        map[topic]?.received?.toEvent(topic)
    }

    /**
     * Obtain the latest sent event.
     * @param topic The topic.
     * @return The latest sent event.
     */
    fun getLatestSentEvent(topic: String) = lock.withLock {
        map[topic]?.sent?.toEvent(topic)
    }

    /**
     * List with all the received events from a topic.
     * @param topic The topic.
     */
    fun getAllReceivedEvents(topic: String): List<Event> = lock.withLock {
        mapAllEventsFromTopic[topic] ?: emptyList()
    }

    /**
     * Add an event to the list of events from a topic.
     * without repetitions of events
     */
    fun setEventToTopic(topic: String, event: Event) = lock.withLock {
        val events = mapAllEventsFromTopic[topic] ?: mutableListOf()
        if (events.contains(event)) return@withLock
        events.add(event)
        mapAllEventsFromTopic[topic] = events
    }

    /**
     * Obtain the next event id.
     * @param topic The topic.
     * @return The next event id.
     */
    fun getNextEventId(topic: String) = lock.withLock {
        val events = map[topic] ?: return@withLock 0L
        max(
            events.received?.id?.plus(1) ?: 0L,
            events.sent?.id?.plus(1) ?: 0L
        )
    }

    /**
     * Defining the latest event received.
     * @param topic The topic of the event.
     * @param received The latest received event.
     */
    fun setLatestReceivedEvent(topic: String, received: Event) = lock.withLock {
        val events = map[topic]?.copy(received = EventInfo(received)) ?: LatestEvents(EventInfo(received), null)
        setEvent(topic, events)
    }

    /**
     * Defining the latest event sent.
     * @param topic The topic of the event.
     * @param sent The latest sent event.
     */
    fun setLatestSentEvent(topic: String, sent: Event) = lock.withLock {
        val events = map[topic]?.copy(sent = EventInfo(sent)) ?: LatestEvents(null, EventInfo(sent))
        setEvent(topic, events)
    }

    /**
     * Define a consumer of a topic.
     * @param topic The topic.
     * @param events The latest received and sent events.
     *
     */
    private fun setEvent(topic: String, events: LatestEvents) = map.set(topic, events)

    /**
     * Obtain all topics and their latest events.
     * @return The list of topics and their latest events.
     */
    fun getAllTopicsAndEvents(): List<Pair<String, Event?>> {
        lock.withLock {
            return map.map { (topic, _) ->
                Pair(topic, getLatestEvent(topic))
            }
        }
    }
}
