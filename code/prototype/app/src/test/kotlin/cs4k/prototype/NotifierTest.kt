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

    @Test
    fun `simple test of one publish one subscribe`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()
        // Act
        notifier.publish(topic, message)
        notifier.subscribe(topic) { event ->
            // Assert
            assertEquals(topic, event.topic)
            assertEquals(message, event.message)
            assertEquals(0, event.id)
        }

    }

    @Test
    fun `One publisher multiple subscribers`() {
        // Arrange
        val message = newMessage()
        val topic = newTopic()
        // Act
        notifier.publish(topic, message)
        val messages: MutableList<String>? = mutableListOf()
        val subscribers = (1..10).map {
            notifier.subscribe(topic) { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                assertEquals(0, event.id)
                messages?.add(event.message)
            }
        }
        // Assert
        assertEquals(10, messages?.size)
        messages?.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `having one subscribe waitting for a message`() {
        // Arrange
        val message = newMessage()
        val topic = newTopic()
        notifier.subscribe(topic) { event ->
            // Assert
            assertEquals(topic, event.topic)
            assertEquals(message, event.message)
            assertEquals(0, event.id)
        }
        // Act
        notifier.publish(topic, message)

    }

    @Test
    fun `having multiple subscribes waitting for a message`() {
        // Arrange
        val message = newMessage()
        val topic = newTopic()
        val messgaes: MutableList<String>? = mutableListOf()
        // Act
        (1..10).map {
            notifier.subscribe(topic) { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                assertEquals(0, event.id)
                messgaes?.add( event.message)
            }
        }
        notifier.publish(topic, message)
        // Assert
        assertEquals(10, messgaes?.size)
        messgaes?.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `one subscribe receiving multiple messages`() {
        // Arrange
        val message = newMessage()
        val message1 = newMessage()
        val message2= newMessage()
        val topic = newTopic()
        val messages: MutableList<String>? = mutableListOf()
        // Act
        notifier.subscribe(topic) { event ->
            // Assert
            assertEquals(topic, event.topic)
            assertEquals(message, event.message)
            assertEquals(0, event.id)
            messages?.add(event.message)
        }
        // Act
        notifier.publish(topic, message)
        notifier.publish(topic, message1)
        notifier.publish(topic, message2)
        // Assert
        assertEquals(3, messages?.size)
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