package cs4k.prototype.services

import cs4k.prototype.broker.Notifier
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class ChatService(val notifier: Notifier) {

    private val generalGroup = "general"

    /**
     * Join a chat group.
     * @param group the optional name of the group.
     */
    fun newListener(group: String = generalGroup): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))
        notifier.subscribe(
            topic = group,
            handler = { event ->
                val sseEmitterEvent = SseEmitter.event()
                    .name(event.topic)
                    .id(event.id.toString())
                    .data("event: ${event.topic} - id: ${event.id} - data: ${event.message}")
                sseEmitter.send(sseEmitterEvent)

                if (event.isLast) sseEmitter.complete()
            }
        )
        return sseEmitter
    }

    /**
     * Send a message to a group.
     * @param message message to send to the group.
     * @param group the optional name of the group.
     */
    fun sendMessage(message: String, group: String = generalGroup) {
        notifier.publish(
            topic = group,
            message = message
        )
    }
}
