package cs4k.prototype.broker.option2

import cs4k.prototype.broker.option2.rabbitmq.experiences.LatestEventStore
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNull

class LatestEventStoreTests {

    @Test
    fun `simple set and get`() {
        // Assemble
        val topic = newRandomTopic()
        val message = newRandomMessage()
        val store = LatestEventStore()

        // Act
        store.createAndSetLatestEvent(topic, message)
        val event = store.getLatestEvent(topic)

        // Assert
        assertEquals(topic, event?.topic)
        assertEquals(message, event?.message)
        assertEquals(0, event?.id)
    }

    @Test
    fun `starting get should be null`() {
        // Assemble
        val store = LatestEventStore()

        // Act
        val event = store.getLatestEvent("topic")

        // Assert
        assertEquals(null, event)
    }

    @Test
    fun `several sets with the same topic should create events with sequencial ids`() {
        // Assemble
        val topic = newRandomTopic()
        val store = LatestEventStore()

        repeat(NUMBER_OF_MESSAGES) {
            // Assemble
            val message = newRandomMessage()

            // Act
            store.createAndSetLatestEvent(topic, message)
            val event = store.getLatestEvent(topic)

            // Assert
            assertEquals(topic, event?.topic)
            assertEquals(message, event?.message)
            assertEquals(it.toLong(), event?.id)
        }
    }

    @Test
    fun `several sets with different topics and removing all should leave store empty`() {
        // Assemble
        val threads = ConcurrentLinkedQueue<Thread>()
        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val store = LatestEventStore()
        val topics = mutableListOf<String>()

        repeat(NUMBER_OF_TOPICS) {
            // Assemble
            val topic = newRandomTopic().also { topics.add(it) }
            val th = Thread {
                try {
                    repeat(NUMBER_OF_MESSAGES) {
                        // Assemble
                        val message = newRandomMessage()

                        // Act
                        store.createAndSetLatestEvent(topic, message)
                        val event = store.getLatestEvent(topic)

                        // Assert
                        assertEquals(topic, event?.topic)
                        assertEquals(message, event?.message)
                        assertEquals(it.toLong(), event?.id)
                    }
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start()
            threads.add(th)
        }

        // Wait for all threads to finish.
        threads.forEach { it.join() }

        // Checking for errors.
        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()

        // Act
        store.removeAll()

        // Assert
        topics.forEach {
            assertNull(store.getLatestEvent(it))
        }
    }

    companion object {
        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"

        private const val NUMBER_OF_TOPICS = 5
        private const val NUMBER_OF_MESSAGES = 100
    }
}
