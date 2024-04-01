package cs4k.prototype.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.Broker
import cs4k.prototype.domain.Message
import cs4k.prototype.http.models.output.MessageOutputModel
import jakarta.annotation.PreDestroy
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class ChatService(val broker: Broker) {

    private val generalGroup = "general"

    @PreDestroy
    private fun clanUp() {
        broker.shutdown()
    }

    /**
     * Join a chat group.
     * @param group The optional name of the group.
     * @return The Spring SSEEmitter.
     */
    fun newListener(group: String?): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(30))
        val unsubscribeCallback = broker.subscribe(
            topic = group ?: generalGroup,
            handler = { event ->
                try {
                    val message = deserializeJsonToMessage(event.message)
                    SseEvent.Message(
                        name = event.topic,
                        id = event.id,
                        data = MessageOutputModel(message.message)
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
     * @param group The optional name of the group.
     * @param message The message to send to the group.
     */
    fun sendMessage(group: String?, message: String) {
        broker.publish(
            topic = group ?: generalGroup,
            message = serializeMessageToJson(Message(message))
        )
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun serializeMessageToJson(message: Message) =
            objectMapper.writeValueAsString(message)

        private fun deserializeJsonToMessage(message: String) =
            objectMapper.readValue(message, Message::class.java)
    }
}
