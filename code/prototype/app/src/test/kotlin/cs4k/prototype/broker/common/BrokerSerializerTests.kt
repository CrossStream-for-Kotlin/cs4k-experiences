package cs4k.prototype.broker.common

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BrokerSerializerTests {

    @Test
    fun `serialize event to json and then deserialize it again to event and check if it is equal`() {
        val originalEvent = Event(
            topic = "topic",
            id = 0,
            message = "message",
            isLast = true
        )
        val serializedEvent = BrokerSerializer.serializeEventToJson(originalEvent)
        val deserializedEvent = BrokerSerializer.deserializeEventFromJson(serializedEvent)

        assertEquals(originalEvent, deserializedEvent)
    }
}