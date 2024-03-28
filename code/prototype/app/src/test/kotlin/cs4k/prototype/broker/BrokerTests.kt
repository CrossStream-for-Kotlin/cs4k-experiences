package cs4k.prototype.broker

import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import org.junit.jupiter.api.AfterAll
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

class BrokerTests {

    @Test
    fun `new subscriber in 1 topic should receive the last message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Act
        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(0, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 topic should receive the same last message even if it is through another broker instance`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert
                    assertEquals(topic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                }
            )
        }
    }

    @Test
    fun `new subscriber in 1 finished topic should receive the last message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = true
        )

        // Act
        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(0, event.id)
                assertEquals(message, event.message)
                assertTrue(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 finished topic should receive the same last message even if it is through another broker instance`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = true
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert
                    assertEquals(topic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(message, event.message)
                    assertTrue(event.isLast)
                }
            )
        }
    }

    @Test
    fun `1 subscriber in 1 topic waiting for 1 message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(0, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
            }
        )

        // Act
        brokerInstances.first().publish(topic, message)
    }

    @Test
    fun `n subscribers in 1 topic waiting for 1 message with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert
                    assertEquals(topic, event.topic)
                    assertEquals(0, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                }
            )
        }

        // Act
        brokerInstances.first().publish(topic, message)
    }

    @Test
    fun `1 subscriber in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()
        val messages = List(NUMBER_OF_MESSAGES) { message }

        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_MESSAGES)

        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                eventsReceived.add(event)
                latch.countDown()
            }
        )

        // Act
        messages.forEach { msg ->
            getRandomBrokerInstance().publish(topic, msg)
        }

        latch.await(1, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.size)
        eventsReceived.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        }
    }

    @Test
    fun `n subscribers in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)

        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
        }

        // Act
        messages.forEach { message ->
            getRandomBrokerInstance().publish(topic, message)
        }

        latch.await(1, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        eventsReceivedSet.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        }
    }

    @Test
    fun `n subscribers in n topics receiving n messages with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)

        // Act
        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                getRandomBrokerInstance().subscribe(
                    topic = entry.key,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
            }
        }

        topicsAndMessages.forEach { entry ->
            entry.value.forEach { message ->
                getRandomBrokerInstance().publish(
                    topic = entry.key,
                    message = message
                )
            }
        }

        latch.await(1, TimeUnit.MINUTES)

        eventsReceived.forEach { event ->
            val entry = topicsAndMessages[event.topic]
            assertNotNull(entry)
            assertContains(entry, event.message)
            assertFalse(event.isLast)
        }
    }

    @Test
    fun `subscriber unsubscribing should not receive message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        val unsubscribe = brokerInstances.first().subscribe(topic) { _ ->
            // Assert
            fail("Event was emitted, however it should have unsubscribed.")
        }

        // Act
        unsubscribe()
        brokerInstances.first().publish(topic, message)

        thread {
            Thread.sleep(4000)
            latch.countDown()
        }

        latch.await(1, TimeUnit.MINUTES)
    }

    @Test
    fun `subscribers unsubscribing should not receive message with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(topic) { _ ->
                // Assert
                fail("Event was emitted, however it should have unsubscribed.")
            }
            unsubscribe()
        }
        brokerInstances.first().publish(topic, message)

        thread {
            Thread.sleep(4000)
            latch.countDown()
        }

        latch.await(1, TimeUnit.MINUTES)
    }

    @Test
    fun `stress test with simultaneous publication of n messages to 1 topic`() {
        // Arrange
        val topic = newRandomTopic()

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)

        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
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
                    getRandomBrokerInstance().publish(
                        topic = topic,
                        message = newRandomMessage()
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }
        threads.forEach { it.join() }

        latch.await(5, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)

        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test with simultaneous publication of n messages to n topics`() {
        // Arrange
        val topics = List(NUMBER_OF_TOPICS) { newRandomTopic() }

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS)

        topics.forEach { topic ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                getRandomBrokerInstance().subscribe(
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
                        getRandomBrokerInstance().publish(
                            topic = topic,
                            message = newRandomMessage()
                        )
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                }
                th.start().also { threads.add(th) }
            }
        }
        threads.forEach { it.join() }

        latch.await(5, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceived.size)
        topics.forEach { topic ->
            val eventsReceivedSet = eventsReceived.filter { it.topic == topic }.toSet()
            assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        }
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test with simultaneous subscription and publication of a message to n topics`() {
        // Arrange
        val topics = List(NUMBER_OF_TOPICS) { newRandomTopic() }
        val message = newRandomMessage()

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_TOPICS)

        // Act
        topics.forEach { topic ->
            val th1 = Thread {
                try {
                    getRandomBrokerInstance().subscribe(
                        topic = topic,
                        handler = { event ->
                            eventsReceived.add(event)
                            latch.countDown()
                        }
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            val th2 = Thread {
                getRandomBrokerInstance().publish(
                    topic = topic,
                    message = message
                )
            }
            th1.start().also { threads.add(th1) }
            th2.start().also { threads.add(th2) }
        }
        threads.forEach { it.join() }

        latch.await(5, TimeUnit.MINUTES)

        // Assert
        assertEquals(NUMBER_OF_TOPICS, eventsReceived.size)

        topics.forEach { topic ->
            val event = eventsReceived.find { event -> event.topic == topic }
            assertNotNull(event)
            assertEquals(message, event.message)
        }
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `cannot invoke method shutdown twice`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Act
        assertFailsWith<BrokerTurnOffException> {
            broker.shutdown()
        }
    }

    @Test
    fun `cannot invoke method subscribe after shutdown`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Act
        assertFailsWith<BrokerTurnOffException> {
            broker.subscribe(newRandomTopic()) { _ -> }
        }
    }

    @Test
    fun `cannot invoke method publish after shutdown`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Act
        assertFailsWith<BrokerTurnOffException> {
            broker.publish(newRandomTopic(), newRandomMessage())
        }
    }

    companion object {
        private const val NUMBER_OF_BROKER_INSTANCES = 4
        private const val NUMBER_OF_TOPICS = 10
        private const val NUMBER_OF_SUBSCRIBERS = 200
        private const val NUMBER_OF_MESSAGES = 200

        private val brokerInstances = List(NUMBER_OF_BROKER_INSTANCES) { Broker() }

        private fun getRandomBrokerInstance() = brokerInstances.random()

        private fun generateRandom() = abs(Random.nextLong())

        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"

        @JvmStatic
        @AfterAll
        fun cleanUp() {
            brokerInstances.forEach { it.shutdown() }
        }
    }
}
