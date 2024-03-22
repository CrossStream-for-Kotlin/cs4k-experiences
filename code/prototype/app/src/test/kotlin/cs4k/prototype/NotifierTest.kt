package cs4k.prototype

import cs4k.prototype.broker.Event
import cs4k.prototype.broker.Notifier
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.math.abs
import kotlin.random.Random
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

    @Test
    fun `new subscriber receives the last message in the topic`() {
        // Arrange
        val newTopic = newTopic()
        val newMessage = newMessage()
        notifier.publish(
            topic = newTopic,
            message = newMessage
        )

        // Act
        notifier.subscribe(
            topic = newTopic,
            handler = { event ->
                // Assert
                assertEquals(newTopic, event.topic)
                assertEquals(0, event.id)
                assertEquals(newMessage, event.message)
            }
        )
    }

    @Test
    fun `new subscribers receives the same last message in the topic`() {
        // Arrange
        val newTopic = newTopic()
        val newMessage = newMessage()
        notifier.publish(
            topic = newTopic,
            message = newMessage
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            notifier.subscribe(
                topic = newTopic,
                handler = { event ->
                    // Assert
                    assertEquals(newTopic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(newMessage, event.message)
                }
            )
        }
    }

    @Test
    fun `1 subscriber waiting for 1 message`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()
        notifier.subscribe(
            topic = topic,
            handler = { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(0, event.id)
                assertEquals(message, event.message)
            }
        )

        // Act
        notifier.publish(topic, message)
    }

    @Test
    fun `n subscribes waiting for 1 message`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            notifier.subscribe(
                topic = topic,
                handler = { event ->
                    // Assert
                    assertEquals(topic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(message, event.message)
                }
            )
        }

        // Act
        notifier.publish(topic, message)
    }

    @Test
    fun `1 subscriber receiving n messages`() {
        // Arrange
        val topic = newTopic()
        val messagesToSend = List(NUMBER_OF_MESSAGES) { newMessage() }

        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_MESSAGES)
        notifier.subscribe(
            topic = topic,
            handler = { event ->
                eventsReceived.add(event)
                latch.countDown()
            }
        )

        // Act
        messagesToSend.forEach { message ->
            notifier.publish(topic, message)
        }

        latch.await(10, TimeUnit.SECONDS)

        // Assert
        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.size)
        eventsReceived.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messagesToSend[idx], event.message)
        }
    }

    @Test
    fun `n subscribers receiving n messages`() {
        // Arrange
        val topic = newTopic()
        val messagesToSend = List(NUMBER_OF_MESSAGES) { newMessage() }

        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        repeat(NUMBER_OF_SUBSCRIBERS) {
            notifier.subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
        }

        // Act
        messagesToSend.forEach { message ->
            notifier.publish(topic, message)
        }

        latch.await(10, TimeUnit.SECONDS)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)
        val eventsReceivedSet = eventsReceived.toSet()
        eventsReceivedSet.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messagesToSend[idx], event.message)
        }
    }

    companion object {
        private val notifier = Notifier()

        private const val NUMBER_OF_SUBSCRIBERS = 30
        private const val NUMBER_OF_MESSAGES = 30

        private fun newTopic() = "topic${abs(Random.nextLong())}"
        private fun newMessage() = "message${abs(Random.nextLong())}"
    }
}