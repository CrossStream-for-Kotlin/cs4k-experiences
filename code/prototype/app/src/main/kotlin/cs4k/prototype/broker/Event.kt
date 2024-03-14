package cs4k.prototype.broker

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

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

class Listener(
    val group: String,
    val sseEmitter: SseEmitter
)
