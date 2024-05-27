package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.option2.rabbitmq.HistoryShareMessage.HistoryShareMessageType.REQUEST
import cs4k.prototype.broker.option2.rabbitmq.HistoryShareMessage.HistoryShareMessageType.RESPONSE

data class HistoryShareMessage(
    val type: HistoryShareMessageType,
    val body: String
) {
    enum class HistoryShareMessageType { REQUEST, RESPONSE }
    fun toRequest(): HistoryShareRequest =
        if (this.type == REQUEST) {
            HistoryShareRequest.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get request out of a response.")
        }
    fun toResponse(): HistoryShareResponse =
        if (this.type == RESPONSE) {
            HistoryShareResponse.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get response out of a request.")
        }
    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: HistoryShareMessage) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareMessage::class.java)
    }
}

fun HistoryShareMessage.toResponse(): HistoryShareResponse =
    if (this.type == RESPONSE) {
        HistoryShareResponse.deserialize(this.body)
    } else {
        throw IllegalArgumentException("Trying to get response out of a request.")
    }

data class HistoryShareRequest(
    val senderQueue: String
) {
    fun toHistoryShareMessage() = HistoryShareMessage(REQUEST, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: HistoryShareRequest) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareRequest::class.java)
    }
}

data class HistoryShareResponse(
    val allConsumeInfo: List<ConsumedTopics.ConsumeInfo>
) {
    fun toHistoryShareMessage() = HistoryShareMessage(RESPONSE, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: HistoryShareResponse) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, HistoryShareResponse::class.java)
    }
}
