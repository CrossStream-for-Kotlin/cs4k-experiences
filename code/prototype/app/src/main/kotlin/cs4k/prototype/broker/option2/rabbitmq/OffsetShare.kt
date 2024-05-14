package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.option2.rabbitmq.ConsumedTopics.ConsumeInfo

class OffsetShareMessage(
    val type: String,
    val message: String
) {
    init {
        require(type == TYPE_REQUEST || type == TYPE_RESPONSE) { "Type not valid." }
    }

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {

        fun createRequest(
            sender: String,
            topic: String
        ): OffsetShareMessage {
            val request = OffsetShareRequest(sender, topic)
            return OffsetShareMessage(TYPE_REQUEST, request.toString())
        }

        fun createResponse(
            offset: Long,
            lastEventId: Long,
            topic: String
        ): OffsetShareMessage {
            val response = OffsetShareResponse(offset, lastEventId, topic)
            return OffsetShareMessage(TYPE_RESPONSE, response.toString())
        }

        const val TYPE_REQUEST = "request"
        const val TYPE_RESPONSE = "response"

        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): OffsetShareMessage =
            objectMapper.readValue(value, OffsetShareMessage::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toOffsetShareMessage() = OffsetShareMessage.deserialize(this)

/**
 * Used to request offset before consuming a given topic, seeing if fellow brokers are already consuming and know
 * the latest offset.
 * @property sender The one sending the request.
 * @property topic The topic about to be consumed.
 */
class OffsetShareRequest(
    val sender: String,
    val topic: String
) {

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): OffsetShareRequest =
            objectMapper.readValue(value, OffsetShareRequest::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toOffsetShareRequest() = OffsetShareRequest.deserialize(this)

/**
 * Used to request offset before consuming a given topic, seeing if fellow brokers are already consuming and know
 * the latest offset.
 * @property sender The one sending the request.
 */
class HistoryShareRequest(
    val sender: String
) {

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): HistoryShareRequest =
            objectMapper.readValue(value, HistoryShareRequest::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toAllOffsetShareRequest() = HistoryShareRequest.deserialize(this)

class OffsetShareResponse(
    val offset: Long,
    val lastEventId: Long,
    val topic: String = ""
) {

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): OffsetShareResponse =
            objectMapper.readValue(value, OffsetShareResponse::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toOffsetShareResponse() = OffsetShareResponse.deserialize(this)

class HistoryShareResponseDeprecated(
    val events: Array<LatestEventInfo>
) {

    class LatestEventInfo(
        val offset: Long,
        val lastEvent: Event
    )

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): HistoryShareResponseDeprecated =
            objectMapper.readValue(value, HistoryShareResponseDeprecated::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toHistoryShareResponseDeprecated() = HistoryShareResponseDeprecated.deserialize(this)

/**
 * Response containing all events stored in a broker.
 * @property events All events that a broker has stored.
 */
class HistoryShareResponse(
    val events: Array<ConsumeInfo>
) {

    override fun toString(): String = objectMapper.writeValueAsString(this)

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        /**
         * Converting the string format into the object.
         * @param value The string form of the request.
         * @return The request object.
         */
        fun deserialize(value: String): HistoryShareResponse =
            objectMapper.readValue(value, HistoryShareResponse::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toAllOffsetShareResponse() = HistoryShareResponse.deserialize(this)
