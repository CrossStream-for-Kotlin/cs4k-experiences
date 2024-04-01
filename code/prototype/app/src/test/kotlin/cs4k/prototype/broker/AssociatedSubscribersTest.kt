package cs4k.prototype.broker

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertTrue

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
        // ASSERT
        assertEquals(associatedSubscribers.getAll(topic).size, 0)
    }

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
        assertEquals(2, associatedSubscribers.getAll(topic).size)
    }

    @Test
    fun `Test of multiple threads adding and removing subscribers`() {
        // Arrange
        val topic = newTopic()
        val subscriberId1 = UUID.randomUUID()
        val subscriberId2 = UUID.randomUUID()
        val subscriber1 = Subscriber(subscriberId1) { _ -> }
        val subscriber2 = Subscriber(subscriberId2) { _ -> }
        var threads = mutableListOf<Thread>()
        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            threads.add(
                Thread { associatedSubscribers.addToKey(topic, subscriber1) },
            )
            threads.add(Thread { associatedSubscribers.addToKey(topic, subscriber2) })
            if (NUMBER_OF_SUBSCRIBERS-1>it){
                threads.add(
                    Thread { associatedSubscribers.removeIf(topic) { it.id == subscriberId1 } },
                )
                threads.add(
                    Thread { associatedSubscribers.removeIf(topic) { it.id == subscriberId2 } },
                )
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        // Assert
        assertTrue( associatedSubscribers.getAll(topic).contains(subscriber1))
        assertTrue( associatedSubscribers.getAll(topic).contains(subscriber2))
    }

    @Test
    fun `Test adding multiple subscribers to the same topic in diferent threads`() {
        // Arrange
        val topic = newTopic()
        val subscribers = mutableListOf<Subscriber>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
            subscribers.add(subscriber)
            val thread = Thread {
                associatedSubscribers.addToKey(topic, subscriber)
            }
            threads.add(thread)
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        //Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS, associatedSubscribers.getAll(topic).size)
    }

    @Test
    fun `Test adding and removing subscribers in a concurrent way`() {
        // Arrange
        val topic = newTopic()
        val subscribers = ConcurrentLinkedQueue<Subscriber>()
        val threads = mutableListOf<Thread>()

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val subscriber = Subscriber(UUID.randomUUID()) { _ -> }
            subscribers.add(subscriber)
            val thread = Thread {
                associatedSubscribers.addToKey(topic, subscriber)
            }
            threads.add(thread)
        }

        repeat(NUMBER_OF_SUBSCRIBERS / 2) {
            val thread = Thread {
                val subscriber = subscribers.poll()
                associatedSubscribers.removeIf(topic) { it.id == subscriber.id }
            }
            threads.add(thread)
        }
        threads.forEach { it.start() }
        threads.forEach { it.join() }

        //Assert
        assertEquals(NUMBER_OF_SUBSCRIBERS / 2, associatedSubscribers.getAll(topic).size)
        associatedSubscribers.getAll(topic).forEach { subscriber ->
            assertEquals(subscribers.contains(subscriber), true)
        }
    }


    companion object {
        private const val NUMBER_OF_SUBSCRIBERS = 50

        private fun newTopic() = "topic${abs(Random.nextLong())}"
    }
}
