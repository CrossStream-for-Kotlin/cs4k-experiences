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
     */
    fun newListener(group: String = generalGroup): Listener {
        val listener = Listener(
            group = group,
            sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))
        )
        notifier.listen(listener)
        return listener
    }

    /**
     * Send a message to a group
     * @param msg message to send to the group
     * @param group the name of the group
     */
    fun sendMessage(msg: String, group: String = generalGroup) {
        notifier.send(group, msg)
    }
}
