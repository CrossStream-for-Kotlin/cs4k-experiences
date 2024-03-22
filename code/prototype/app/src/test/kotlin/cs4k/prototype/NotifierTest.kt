package cs4k.prototype

import cs4k.prototype.broker.Notifier
import kotlin.test.Test
import kotlin.test.assertEquals

class NotifierTest {
    val not = Notifier()

    @Test
    fun `simple test of one publish one subscribe`() {
        not.publish("topic", "message")
        not.subscribe("topic") { event ->
            assertEquals("topic", event.topic)
            assertEquals("message", event.message)
            println(event.message)
        }

    }

    @Test
    fun `One publisher multiple subscribers`() {
        val message = "1a"
        val topic = "Jogo"
        not.publish(topic, message)
        val messages: MutableList<String>? = mutableListOf()
        val subscribers = (1..10).map {
            not.subscribe(topic) { event ->
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                messages?.add(event.message)
            }
        }
        assertEquals(10, messages?.size)
        messages?.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `having one subscribe waitting for a message`() {
        val message = "2b"
        val topic = "Jogo4"
        not.subscribe(topic) { event ->
            assertEquals(topic, event.topic)
            assertEquals(message, event.message)
        }
        not.publish(topic, message)

    }

    @Test
    fun `having multiple subscribes waitting for a message`() {
        val message = "2b"
        val topic = "Jogo4"
        val messgaes: MutableList<String>? = mutableListOf()
        val subscribers = (1..10).map {
            not.subscribe(topic) { event ->
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                messgaes?.add( event.message)
            }
        }
        not.publish(topic, message)
        assertEquals(10, messgaes?.size)
        messgaes?.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `one subscribe receiving multiple messages`() {
        val message = "2b"
        val message1 = "2c"
        val message2= "2d"
        val topic = "Jogo4"
        val messages: MutableList<String>? = mutableListOf()
        not.subscribe(topic) { event ->
            assertEquals(topic, event.topic)
            messages?.add( event.message)
        }
        not.publish(topic, message)
        not.publish(topic, message1)
        not.publish(topic, message2)
        assertEquals(3, messages?.size)
        messages?.forEach() {
            println(it)
        }
        assertEquals(message, messages?.get(1))
        assertEquals(message1, messages?.get(2))
        assertEquals(message2, messages?.get(0))
    }

}