package cs4k.prototype.broker.option2.rabbitmq

import cs4k.prototype.broker.option2.rabbitmq.experiences.ConsumedTopicsDepreciated
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ConsumedTopicsDepreciatedTests {

    @Test
    fun `event - simple set and get`() {
        // Assemble
        val topic = newRandomTopic()
        val message = newRandomMessage()
        val store = ConsumedTopicsDepreciated()
        // Act
        store.createAndSetLatestEvent(topic, message)
        val event = store.getLatestEvent(topic)

        // Assert
        assertEquals(topic, event?.topic)
        assertEquals(message, event?.message)
        assertEquals(0, event?.id)
    }

    @Test
    fun `event - starting get should be null`() {
        // Assemble
        val store = ConsumedTopicsDepreciated()

        // Act
        val event = store.getLatestEvent("topic")

        // Assert
        assertEquals(null, event)
    }

    @Test
    fun `event - several sets with the same topic should create events with sequencial ids`() {
        // Assemble
        val topic = newRandomTopic()
        val store = ConsumedTopicsDepreciated()

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
    fun `event - several sets with different topics and removing all should leave store empty`() {
        // Assemble
        val threads = ConcurrentLinkedQueue<Thread>()
        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Throwable>()
        val store = ConsumedTopicsDepreciated()
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

    @Test
    fun `offset - simple set and get`() {
        // Assemble
        val topic = newRandomTopic()
        val message = generateRandom()
        val store = ConsumedTopicsDepreciated()
        // Act
        store.setOffset(topic, message)
        val offset = store.getOffsetNoWait(topic)

        // Assert
        assertEquals(message, offset)
    }

    @Test
    fun `offset - starting get should be null`() {
        // Assemble
        val store = ConsumedTopicsDepreciated()

        // Act
        val event = store.getOffsetNoWait("topic")

        // Assert
        assertEquals(null, event)
    }

    @Test
    fun `offset - first passive waiting get should set offset`() {
        // Assemble
        val store = ConsumedTopicsDepreciated()
        val offset = generateRandom()
        val offsetSetter = { _: String -> store.setOffset(topic = "", offset) }

        // Act
        val fetchedOffset = store.getOffset("", fetchOffset = offsetSetter)

        // Assert
        assertEquals(offset, fetchedOffset)
    }

    companion object {
        private fun generateRandom() = abs(Random.nextLong())
        private fun newRandomTopic() = "topic${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"

        private const val NUMBER_OF_TOPICS = 5
        private const val NUMBER_OF_MESSAGES = 100
    }
}
