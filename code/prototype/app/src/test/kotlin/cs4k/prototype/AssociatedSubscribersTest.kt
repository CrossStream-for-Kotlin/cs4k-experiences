package cs4k.prototype

import cs4k.prototype.broker.AssociatedSubscribers
import cs4k.prototype.broker.Subscriber
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.math.abs
import kotlin.random.Random

class AssociatedSubscribersTest {
    private val associatedSubscribers = AssociatedSubscribers()

    @Test
    fun `insert and get a subscribe `() {
        // ACT
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = Subscriber(subscriberId) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber)
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(1, subscribers.size)
        assertEquals(subscriber, subscribers[0])
    }

    @Test
    fun `insert and remove a subscribe `() {
        // ACT
        val topic = newTopic()
        val subscriberId = UUID.randomUUID()
        val subscriber = Subscriber(subscriberId) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber)
        associatedSubscribers.removeIf(topic) { it.id == subscriberId }
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(0, subscribers.size)
    }

    @Test
    fun `insert and remove a subscribe with multiple subscribers`() {
        // ACT
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic) { it.id == subscriberId1 }
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(1, subscribers.size)
        assertEquals(subscriber2, subscribers[0])
    }

    @Test
    fun `insert and remove a subscribe with multiple subscribers and remove the last one`() {
        // ACT
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic) { it.id == subscriberId2 }
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(1, subscribers.size)
        assertEquals(subscriber1, subscribers[0])
    }

    @Test
    fun `insert and remove a subscribe with multiple subscribers and remove the first one`() {
        // ACT
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        associatedSubscribers.removeIf(topic) { it.id == subscriberId1 }
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(1, subscribers.size)
        assertEquals(subscriber2, subscribers[0])
    }

    @Test
    fun `insert and remove a subscribe with multiple subscribers and remove the first one in a concurrent way`() {
        // ACT
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        // ARRANGE
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)
        val thread1 = Thread {
            associatedSubscribers.removeIf(topic) { it.id == subscriberId1 }
        }
        val thread2 = Thread {
            associatedSubscribers.removeIf(topic) { it.id == subscriberId2 }
        }
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()
        val subscribers = associatedSubscribers.getAll(topic)
        // ASSERT
        assertEquals(0, subscribers.size)
    }

    // multiple threads adding and removing subscribers
    @Test
    fun `Test to insert and remove a subscribe with multiple subscribers and remove the first one in a concurrent way 2`() {
        // Arrange
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        associatedSubscribers.addToKey(topic, subscriber1)
        associatedSubscribers.addToKey(topic, subscriber2)

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val threads = listOf(
                Thread { associatedSubscribers.removeIf(topic) { it.id == subscriberId1 } },
                Thread { associatedSubscribers.removeIf(topic) { it.id == subscriberId2 } },
                Thread { associatedSubscribers.addToKey(topic, subscriber1) },
                Thread { associatedSubscribers.addToKey(topic, subscriber2) }
            )

            threads.forEach(Thread::start)
            threads.forEach(Thread::join)
        }

        // Assert
        val subscribers = associatedSubscribers.getAll(topic)
        assertEquals(2, subscribers.size)
    }

    companion object {
        private const val NUMBER_OF_SUBSCRIBERS = 50

        private fun newTopic() = "topic${abs(Random.nextLong())}"
    }
}
