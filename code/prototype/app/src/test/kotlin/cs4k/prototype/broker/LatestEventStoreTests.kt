package cs4k.prototype.broker

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class LatestEventStoreTests {

    @Test
    fun `simple test - able to get the first message from topic`() {
        // Assemble
        val store = LatestEventStore()
        val messageSent = Event(newRandomTopic(), generateRandom(), newRandomMessage())
        // Act
        store.setLatestEvent(messageSent.topic, messageSent)
        val messageReceived = store.getLatestEvent(messageSent.topic)
        // Assert
        assertEquals(messageSent, messageReceived)
    }

    @Test
    fun `set and get done in different threads`() {
        // Assert
        val store = LatestEventStore()
        val messageSent = Event(newRandomTopic(), generateRandom(), newRandomMessage())
        // Act
        val publishingThread = Thread {
            store.setLatestEvent(messageSent.topic, messageSent)
        }
        publishingThread.start()
        val messageReceived = store.getLatestEvent(messageSent.topic)
        publishingThread.join()
        // Assert
        assertEquals(messageSent, messageReceived)
    }

    @Test
    fun `one set should be seen by all getters`() {
        // Assemble
        val store = LatestEventStore()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val threads = ConcurrentLinkedQueue<Thread>()

        val messageSent = Event(newRandomTopic(), generateRandom(), newRandomMessage())
        val publishingThread = Thread {
            store.setLatestEvent(messageSent.topic, messageSent)
        }
        publishingThread.start()
        repeat(NUMBER_OF_GETTERS) {
            Thread {
                try {
                    // Act
                    val messageReceived = store.getLatestEvent(messageSent.topic)
                    // Assert
                    assertEquals(messageSent, messageReceived)
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }.also { it.start(); threads.add(it) }
        }
        publishingThread.join()
        threads.forEach { it.join() }

        // Checking for assertion errors.
        if (errors.isNotEmpty()) {
            throw errors.peek()
        }
        if (failures.isNotEmpty()) {
            throw failures.peek()
        }
    }

    @Test
    fun `periodic set should always be seen by getters`() {
        // Assemble
        val store = LatestEventStore()

        val settingEventLock = ReentrantLock()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val threads = ConcurrentLinkedQueue<Thread>()

        val topic = newRandomTopic()

        val messages =
            List((TEST_DURATION_MILIS / SET_DELAY_MILIS).toInt()) { Event(topic, it.toLong(), newRandomMessage()) }
        var lastMessageSent: Event? = null

        val publishingThread = Thread {
            messages.forEach { messageSent ->
                settingEventLock.withLock {
                    store.setLatestEvent(topic, messageSent)
                    lastMessageSent = messageSent
                }
            }
            Thread.sleep(SET_DELAY_MILIS)
        }

        publishingThread.start()

        val startingTime = System.currentTimeMillis()

        repeat(NUMBER_OF_GETTERS) {
            val thread = Thread {
                while (true) {
                    try {
                        settingEventLock.withLock {
                            // Act
                            val messageReceived = store.getLatestEvent(topic)
                            // Assert
                            assertEquals(lastMessageSent, messageReceived)
                        }
                    } catch (e: AssertionError) {
                        failures.add(e)
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                    val endTime = System.currentTimeMillis()
                    if (endTime - startingTime >= TEST_DURATION_MILIS) break
                }
            }
            thread.start().also { threads.add(thread) }
        }

        threads.forEach { it.join() }
        publishingThread.interrupt()
        publishingThread.join()

        // Checking for assertion errors.
        if (errors.isNotEmpty()) {
            throw errors.peek()
        }
        if (failures.isNotEmpty()) {
            throw failures.peek()
        }
    }

    @Test
    fun `multiple setters and multiple getters`() {
        // Assemble
        val store = LatestEventStore()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()

        val pushingThreads = ConcurrentLinkedQueue<Thread>()
        val pollingThreads = ConcurrentLinkedQueue<Thread>()

        val topic = newRandomTopic()

        val messages = List((TEST_DURATION_MILIS / SET_DELAY_MILIS).toInt()) {
            Event(topic, it.toLong(), newRandomMessage())
        }

        repeat(NUMBER_OF_SETTERS) {
            val thread = Thread {
                messages.forEach { messageSent ->
                    store.setLatestEvent(topic, messageSent)
                    Thread.sleep(SET_DELAY_MILIS)
                }
            }
            thread.start().also { pushingThreads.add(thread) }
        }

        val startingTime = System.currentTimeMillis()

        repeat(NUMBER_OF_GETTERS) {
            Thread {
                try {
                    val receivedMessages = HashSet<Event>()
                    while (true) {
                        try {
                            // Act
                            val messageReceived = store.getLatestEvent(topic)
                            assertNotNull(messageReceived)
                            receivedMessages.add(messageReceived)
                        } catch (e: AssertionError) {
                            failures.add(e)
                        } catch (e: Exception) {
                            errors.add(e)
                        }
                        val endTime = System.currentTimeMillis()
                        if (endTime - startingTime >= TEST_DURATION_MILIS) break
                    }
                    // Assert
                    assertEquals(messages.size, receivedMessages.size)
                    assertContentEquals(messages, receivedMessages.toList().sortedBy { it.id })
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }.also { it.start(); pollingThreads.add(it) }
        }

        pushingThreads.forEach { it.join() }
        pollingThreads.forEach { it.join() }
        // Checking for assertion errors.
        if (errors.isNotEmpty()) {
            throw errors.peek()
        }
        if (failures.isNotEmpty()) {
            throw failures.peek()
        }
    }

    companion object {
        private const val NUMBER_OF_GETTERS = 20
        private const val NUMBER_OF_SETTERS = 5
        private const val SET_DELAY_MILIS = 1000L
        private const val TEST_DURATION_MILIS = 30000L

        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"
    }
}
