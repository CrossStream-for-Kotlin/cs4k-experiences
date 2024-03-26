package cs4k.prototype.broker

/**
 * Represents an event that can be published to a topic.
 * @param topic the topic of the event.
 * @param id the id of the event.
 * @param message the message of the event.
 * @param isLast if the event is the last one.
 */
data class Event(
    val topic: String,
    val id: Long,
    val message: String,
    val isLast: Boolean = false
)

/*
sealed interface Event {

    fun writeTo(emitter: SseEmitter)

    class Message(eventName: String, id: Long, data: String) : Event {

        private val event = SseEmitter.event()
            .name(eventName)
            .id(id.toString())
            .data("event: $eventName - id: $id - data: $data")

        override fun writeTo(emitter: SseEmitter) {
            emitter.send(
                event
            )
        }
    }

    class KeepAliveV1(timestamp: Long) : Event {

        private val event = SseEmitter.event()
            .comment(timestamp.toString())

        override fun writeTo(emitter: SseEmitter) {
            emitter.send(
                event
            )
        }
    }
}
*/
