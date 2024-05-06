package cs4k.prototype.broker.option2.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

/**
 * Used to request offset before consuming a given topic, seeing if fellow brokers are already consuming and know
 * the latest offset.
 * @property sender The one sending the request.
 * @property senderQueue Queue name to publish the response to.
 * @property topic The topic about to be consumed.
 */
class OffsetSharingRequest(
    val sender: String,
    val senderQueue: String,
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
        fun deserialize(value: String): OffsetSharingRequest =
            objectMapper.readValue(value, OffsetSharingRequest::class.java)
    }
}

/**
 * Converting the string format into the object.
 * @receiver The string form of the request.
 * @return The request object.
 */
fun String.toOffsetSharingRequest() = OffsetSharingRequest.deserialize(this)
