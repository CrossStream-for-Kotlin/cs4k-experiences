package cs4k.prototype

import cs4k.prototype.broker.Event
import cs4k.prototype.broker.Notifier
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

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

    /*
    This test MAY fail as a subscriber may receive the same message twice.
    Reasons are unknown as of now. However, it isn't problematic as it is just a repeat of a message already received.
     */
    @Test
    fun `One publisher multiple subscribers`() {
        // Arrange
        val message = newMessage()
        val topic = newTopic()
        // Act
        notifier.publish(topic, message)
        val messages: MutableList<String> = mutableListOf()
        (1..10).map {
            notifier.subscribe(topic) { event ->
                // Assert
                logger.info("testing subscriber number $it")
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                assertEquals(0, event.id)
                messages.add(event.message)
            }
        }
        // Assert
        assertEquals(10, messages.size)
        messages.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `having one subscribe waiting for a message`() {
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
    fun `having multiple subscribes waiting for a message`() {
        // Arrange
        val message = newMessage()
        val topic = newTopic()
        val messages: MutableList<String> = mutableListOf()
        val latch = CountDownLatch(10)
        // Act
        (1..10).map {
            notifier.subscribe(topic) { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(message, event.message)
                assertEquals(0, event.id)
                messages.add(event.message)
                latch.countDown()
            }
        }
        notifier.publish(topic, message)
        latch.await(10, TimeUnit.SECONDS)
        // Assert
        assertEquals(10, messages.size)
        messages.forEach {
            assertEquals(message, it)
        }
    }

    @Test
    fun `one subscribe receiving multiple messages`() {
        // Arrange
        val message = newMessage()
        val message1 = newMessage()
        val message2 = newMessage()
        val topic = newTopic()
        val messages: MutableList<String> = mutableListOf()
        val latch = CountDownLatch(3)
        // Act
        notifier.subscribe(topic) { event ->
            // Assert
            assertEquals(topic, event.topic)
            assertTrue(event.id in (0L..2L))
            when (event.id) {
                0L -> assertEquals(message, event.message)
                1L -> assertEquals(message1, event.message)
                2L -> assertEquals(message2, event.message)
                else -> IllegalStateException("how did you make it past?")
            }

            messages.add(event.message)
            latch.countDown()
        }
        // Act
        notifier.publish(topic, message)
        notifier.publish(topic, message1)
        notifier.publish(topic, message2)

        latch.await(10, TimeUnit.SECONDS)

        // Assert
        assertEquals(3, messages.size)
        assertEquals(message, messages[0])
        assertEquals(message1, messages[1])
        assertEquals(message2, messages[2])
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

    @Test
    fun `subscriber unsubscribing should not receive message`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()
        val latch = CountDownLatch(1)

        val unsubscribe = notifier.subscribe(topic) { event ->
            // Assert
            fail("Event was emitted, however it should have unsubscribed.")
        }
        // Act
        unsubscribe()
        notifier.publish(topic, message)

        thread {
            Thread.sleep(2000)
            latch.countDown()
        }

        latch.await(3, TimeUnit.SECONDS)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(NotifierTest::class.java)

        private val notifier = Notifier()

        private const val NUMBER_OF_SUBSCRIBERS = 30
        private const val NUMBER_OF_MESSAGES = 30

        private fun newTopic() = "topic${abs(Random.nextLong())}"
        private fun newMessage() = "message${abs(Random.nextLong())}"
    }
}
