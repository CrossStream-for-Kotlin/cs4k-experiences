package cs4k.prototype.services

import cs4k.prototype.broker.Notifier
import cs4k.prototype.services.SseEvent.Message
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class ChatService(val notifier: Notifier) {

    private val generalGroup = "general"

    /**
     * Join a chat group.
     * @param group the optional name of the group.
     * @return the Spring SSEEmitter.
     */
    fun newListener(group: String?): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        val unsubscribeCallback = notifier.subscribe(
            topic = group ?: generalGroup,
            handler = { event ->
                try {
                    Message(event.topic, event.id, event.message).writeTo(sseEmitter)
                    if (event.isLast) sseEmitter.complete()
                } catch (ex: Exception) {
                    sseEmitter.completeWithError(ex)
                }
            }
        )

        sseEmitter.onCompletion {
            unsubscribeCallback()
        }
        sseEmitter.onError {
            unsubscribeCallback()
        }

        return sseEmitter
    }

    /**
     * Send a message to a group.
     * @param group the optional name of the group.
     * @param message message to send to the group.
     */
    fun sendMessage(group: String?, message: String) {
        notifier.publish(
            topic = group ?: generalGroup,
            message = message
        )
    }
}
