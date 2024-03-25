package cs4k.prototype.broker

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail

class NotifierTests {

    @Test
    fun `new subscriber in 1 topic should receive the last message`() {
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
                assertFalse(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 topic should receive the same last message`() {
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
                    assertFalse(event.isLast)
                }
            )
        }
    }

    @Test
    fun `new subscriber in 1 finished topic should receive the last message`() {
        // Arrange
        val newTopic = newTopic()
        val newMessage = newMessage()

        notifier.publish(
            topic = newTopic,
            message = newMessage,
            isLastMessage = true
        )

        // Act
        notifier.subscribe(
            topic = newTopic,
            handler = { event ->
                // Assert
                assertEquals(newTopic, event.topic)
                assertEquals(0, event.id)
                assertEquals(newMessage, event.message)
                assertTrue(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 finished topic should receive the same last message`() {
        // Arrange
        val newTopic = newTopic()
        val newMessage = newMessage()

        notifier.publish(
            topic = newTopic,
            message = newMessage,
            isLastMessage = true
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
                    assertTrue(event.isLast)
                }
            )
        }
    }

    @Test
    fun `1 subscriber in 1 topic waiting for 1 message`() {
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
    fun `n subscribers in 1 topic waiting for 1 message`() {
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
    fun `1 subscriber in 1 topic receiving n messages`() {
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

        latch.await(1, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.size)
        eventsReceived.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messagesToSend[idx], event.message)
        }
    }

    @Test
    fun `n subscribers in 1 topic receiving n messages`() {
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

        latch.await(1, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        eventsReceivedSet.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messagesToSend[idx], event.message)
        }
    }

    @Test
    fun `n subscribers in n topic receiving n messages`() {
        // Arrange
        val topicsAndMessages = List(NUMBER_OF_TOPICS) { Pair(newTopic(), newMessage()) }
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_TOPICS)

        // Act
        topicsAndMessages.forEach { topic ->
            notifier.subscribe(
                topic = topic.first,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
            notifier.publish(
                topic = topic.first,
                message = topic.second
            )
        }

        latch.await(1, TimeUnit.MINUTES)

        eventsReceived.forEach { event ->
            val msg = topicsAndMessages.find { it.first == event.topic }?.second
            assertEquals(msg, event.message)
        }
    }

    @Test
    fun `subscriber unsubscribing should not receive message`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()
        val latch = CountDownLatch(1)

        val unsubscribe = notifier.subscribe(topic) { _ ->
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

        latch.await(1, TimeUnit.MINUTES)
    }

    @Test
    fun `subscribers unsubscribing should not receive message`() {
        // Arrange
        val topic = newTopic()
        val message = newMessage()
        val latch = CountDownLatch(1)

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = notifier.subscribe(topic) { _ ->
                // Assert
                fail("Event was emitted, however it should have unsubscribed.")
            }
            unsubscribe()
        }
        notifier.publish(topic, message)

        thread {
            Thread.sleep(2000)
            latch.countDown()
        }

        latch.await(1, TimeUnit.MINUTES)
    }

    @Test
    fun `stress test with simultaneous sending n messages to 1 topic`() {
        // Error control
        val errors = ConcurrentLinkedQueue<Exception>()

        // Arrange
        val topic = newTopic()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val ths = ConcurrentLinkedQueue<Thread>()
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
        repeat(NUMBER_OF_MESSAGES) {
            val th = Thread {
                try {
                    notifier.publish(
                        topic = topic,
                        message = newMessage()
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start()
            ths.add(th)
        }
        ths.forEach { it.join() }

        latch.await(3, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)

        // Error checking
        if (!errors.isEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test with simultaneous sending n messages to n topic`() {
        // Error control
        val errors = ConcurrentLinkedQueue<Exception>()

        // Arrange
        val topics = List(NUMBER_OF_TOPICS) { newTopic() }
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val ths = ConcurrentLinkedQueue<Thread>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS)

        topics.forEach { topic ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                notifier.subscribe(
                    topic = topic,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
            }
        }

        // Act
        topics.forEach { topic ->
            repeat(NUMBER_OF_MESSAGES) {
                val th = Thread {
                    try {
                        notifier.publish(
                            topic = topic,
                            message = newMessage()
                        )
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                }
                th.start()
                ths.add(th)
            }
        }
        ths.forEach { it.join() }

        latch.await(5, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceived.size)
        topics.forEach { topic ->
            val eventsReceivedSet = eventsReceived.filter { it.topic == topic }.toSet()
            assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        }

        // Error checking
        if (!errors.isEmpty()) throw errors.peek()
    }

    companion object {
        private val notifier = Notifier()

        private const val NUMBER_OF_TOPICS = 2
        private const val NUMBER_OF_SUBSCRIBERS = 50
        private const val NUMBER_OF_MESSAGES = 50

        private fun newTopic() = "topic${abs(Random.nextLong())}"
        private fun newMessage() = "message${abs(Random.nextLong())}"
    }
}
