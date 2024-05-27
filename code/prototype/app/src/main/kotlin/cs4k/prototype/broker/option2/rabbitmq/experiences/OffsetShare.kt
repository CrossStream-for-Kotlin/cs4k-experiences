package cs4k.prototype.broker.option2.rabbitmq.experiences

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.option2.rabbitmq.experiences.OffsetShareMessage.OffsetShareRequestType.REQUEST
import cs4k.prototype.broker.option2.rabbitmq.experiences.OffsetShareMessage.OffsetShareRequestType.RESPONSE

data class OffsetShareMessage(
    val type: OffsetShareRequestType,
    val body: String
) {
    enum class OffsetShareRequestType { REQUEST, RESPONSE }

    fun toRequest(): OffsetShareRequest =
        if (this.type == REQUEST) {
            OffsetShareRequest.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get request out of a response.")
        }

    fun toResponse(): OffsetShareResponse =
        if (this.type == RESPONSE) {
            OffsetShareResponse.deserialize(this.body)
        } else {
            throw IllegalArgumentException("Trying to get response out of a request.")
        }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: OffsetShareMessage) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, OffsetShareMessage::class.java)
    }
}

data class OffsetShareRequest(
    val senderQueue: String,
    val topic: String
) {
    fun toMessage() = OffsetShareMessage(REQUEST, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: OffsetShareRequest) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, OffsetShareRequest::class.java)
    }
}

data class OffsetShareResponse(
    val offset: Long,
    val lastEventId: Long,
    val topic: String
) {
    fun toMessage() = OffsetShareMessage(RESPONSE, serialize(this))

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        fun serialize(value: OffsetShareResponse) = objectMapper.writeValueAsString(value)
        fun deserialize(string: String) = objectMapper.readValue(string, OffsetShareResponse::class.java)
    }
}
