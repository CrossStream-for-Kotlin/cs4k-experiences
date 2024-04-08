package cs4k.prototype.broker

import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.BrokerException.DbConnectionPoolSizeException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.RepeatedTest
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
    fun `cannot create a broker with a negative database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<DbConnectionPoolSizeException> {
            Broker(dbConnectionPoolSize = -10)
        }
    }

    @Test
    fun `cannot create a broker with too high a database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<DbConnectionPoolSizeException> {
            Broker(dbConnectionPoolSize = 1000)
        }
    }

    @Test
    fun `new subscriber in 1 topic should receive the last message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = false
        )

        // Act
        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert
                assertEquals(topic, event.topic)
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 topic should receive the same last message even with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = false
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert
                    assertEquals(topic, event.topic)
                    assertEquals(FIRST_EVENT_ID, event.id)
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
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertTrue(event.isLast)
            }
        )
    }

    @Test
    fun `new subscribers in 1 finished topic should receive the same last message even with several broker instances involved`() {
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
                    assertEquals(FIRST_EVENT_ID, event.id)
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
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
            }
        )

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )
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
                    assertEquals(FIRST_EVENT_ID, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                }
            )
        }

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )
    }

    @Test
    fun `1 subscriber in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_MESSAGES)
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                eventsReceived.add(event)
                latch.countDown()
            }
        )

        // Act
        messages.forEach { msg ->
            getRandomBrokerInstance().publish(
                topic = topic,
                message = msg
            )
        }

        // Assert
        latch.await(1, TimeUnit.MINUTES)

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

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val eventsReceived = ConcurrentLinkedQueue<Event>()

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
            getRandomBrokerInstance().publish(
                topic = topic,
                message = message
            )
        }

        // Assert
        latch.await(1, TimeUnit.MINUTES)

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

        val latch = CountDownLatch(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val eventsReceived = ConcurrentLinkedQueue<Event>()

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

        // Assert
        latch.await(1, TimeUnit.MINUTES)

        assertEquals(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

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

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

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

        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        thread {
            Thread.sleep(4000)
            latch.countDown()
        }

        latch.await(1, TimeUnit.MINUTES)
    }

    @RepeatedTest(3)
    fun `stress test with simultaneous publication of n messages to 1 topic with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

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
            val th = Thread {
                try {
                    getRandomBrokerInstance().publish(
                        topic = topic,
                        message = message
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }
        threads.forEach { it.join() }

        // Assert
        latch.await(5, TimeUnit.MINUTES)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived
            .toSet()
            .map { event -> event.message }
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        assertTrue(eventsReceivedSet.containsAll(messages))

        if (errors.isNotEmpty()) throw errors.peek()
    }

    @RepeatedTest(3)
    fun `stress test with simultaneous publication of n messages to n topics with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS)

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

        // Act
        topicsAndMessages.forEach { entry ->
            entry.value.forEach { message ->
                val th = Thread {
                    try {
                        getRandomBrokerInstance().publish(
                            topic = entry.key,
                            message = message
                        )
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                }
                th.start().also { threads.add(th) }
            }
        }
        threads.forEach { it.join() }

        // Assert
        latch.await(5, TimeUnit.MINUTES)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS, eventsReceived.size)
        topicsAndMessages.forEach { entry ->
            val eventsReceivedSet = eventsReceived
                .filter { it.topic == entry.key }
                .toSet()
                .map { event -> event.message }
            assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)

            val messages = topicsAndMessages[entry.key]
            assertNotNull(messages)
            assertTrue(eventsReceivedSet.containsAll(messages))
        }

        if (errors.isNotEmpty()) throw errors.peek()
    }

    @RepeatedTest(3)
    fun `stress test with simultaneous subscription and publication of a message to n topics`() {
        // Arrange
        val topicsAndMessages = List(NUMBER_OF_TOPICS) { Pair(newRandomTopic(), newRandomMessage()) }

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_TOPICS)

        // Act
        topicsAndMessages.forEach { pair ->
            val th1 = Thread {
                try {
                    getRandomBrokerInstance().subscribe(
                        topic = pair.first,
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
                    topic = pair.first,
                    message = pair.second
                )
            }
            th1.start().also { threads.add(th1) }
            th2.start().also { threads.add(th2) }
        }
        threads.forEach { it.join() }

        // Assert
        latch.await(5, TimeUnit.MINUTES)

        assertTrue(eventsReceived.size >= NUMBER_OF_TOPICS)

        topicsAndMessages.forEach { pair ->
            val event = eventsReceived.find { event -> event.topic == pair.first }
            assertNotNull(event)
            assertEquals(pair.second, event.message)
        }

        if (errors.isNotEmpty()) throw errors.peek()
    }


    @Test
    fun `cannot invoke method shutdown twice`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.shutdown()
        }
    }

    @Test
    fun `cannot invoke method subscribe after shutdown`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.subscribe(newRandomTopic()) { _ -> }
        }
    }

    @Test
    fun `cannot invoke method publish after shutdown`() {
        // Arrange
        val broker = Broker()
        broker.shutdown()

        // Assert
        assertFailsWith<BrokerTurnOffException> {
            // Act
            broker.publish(newRandomTopic(), newRandomMessage())
        }
    }

    companion object {
        private const val FIRST_EVENT_ID = 0L
        private const val NUMBER_OF_BROKER_INSTANCES = 5
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
