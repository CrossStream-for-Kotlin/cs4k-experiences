package cs4k.prototype.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.Broker
import cs4k.prototype.domain.Message
import cs4k.prototype.http.models.output.MessageOutputModel
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class ChatService(val broker: Broker) {

    private val generalGroup = "general"

    /**
     * Join a chat group.
     * @param group the optional name of the group.
     * @return the Spring SSEEmitter.
     */
    fun newListener(group: String?): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        val unsubscribeCallback = broker.subscribe(
            topic = group ?: generalGroup,
            handler = { event ->
                try {
                    SseEvent.Message(
                        name = event.topic,
                        id = event.id,
                        data = MessageOutputModel(deserializeJsonToMessage(event.message).message)
                    ).writeTo(
                        sseEmitter
                    )

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
        broker.publish(
            topic = group ?: generalGroup,
            message = serializeMessageToJson(Message(message))
        )
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun serializeMessageToJson(message: Message) = objectMapper.writeValueAsString(message)
        private fun deserializeJsonToMessage(message: String) = objectMapper.readValue(message, Message::class.java)
    }
}
