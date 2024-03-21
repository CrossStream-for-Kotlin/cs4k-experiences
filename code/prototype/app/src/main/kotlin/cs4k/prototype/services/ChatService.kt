package cs4k.prototype.services

import cs4k.prototype.broker.Listener
import cs4k.prototype.broker.Notifier
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class ChatService(val notifier: Notifier) {

    private val generalGroup = "general"

    /**
     * Join a chat group
     * @param group the name of the group
     *
     */
    fun newListener(group: String = generalGroup): SseEmitter {
        val emitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))
        notifier.subscribe(
            topic = group,
            callback = { event, toComplete ->

                val sseEmitterEvent = SseEmitter.event()
                    .name(event.topic)
                    .id(event.message)
                    .data("event: ${event.topic} - id: ${event.id} - data: ${event.message}")

                emitter.send(sseEmitterEvent)
                if (toComplete) {
                    emitter.complete()
                }
            }
        )
        return emitter
    }

    /**
     * Send a message to a group
     * @param msg message to send to the group
     * @param group the name of the group
     */
    fun sendMessage(msg: String, group: String = generalGroup) {
        notifier.publish(
            topic = group,
            message = msg
        )
    }
}
