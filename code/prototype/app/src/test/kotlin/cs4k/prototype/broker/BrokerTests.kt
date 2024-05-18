package cs4k.prototype.broker

import cs4k.prototype.broker.common.BrokerException.BrokerTurnOffException
import cs4k.prototype.broker.common.Event
import cs4k.prototype.broker.option1.BrokerSQL
import org.junit.jupiter.api.AfterAll
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
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

        val latch = CountDownLatch(1)

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = false
        )

        // Act
        val unsubscribe = brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert [1]
                assertEquals(topic, event.topic)
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
                latch.countDown()
            }
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `new subscribers in 1 topic should receive the same last message even with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS)
        val unsubscribes = mutableListOf<() -> Unit>()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = false
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert [1]
                    assertEquals(topic, event.topic)
                    assertEquals(FIRST_EVENT_ID, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `new subscriber in 1 finished topic should receive the last message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = true
        )

        // Act
        val unsubscribe = brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert [1]
                assertEquals(topic, event.topic)
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertTrue(event.isLast)
                latch.countDown()
            }
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `new subscribers in 1 finished topic should receive the same last message even with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS)
        val unsubscribes = mutableListOf<() -> Unit>()

        brokerInstances.first().publish(
            topic = topic,
            message = message,
            isLastMessage = true
        )

        // Act
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert [1]
                    assertEquals(topic, event.topic)
                    assertEquals(FIRST_EVENT_ID, event.id)
                    assertEquals(message, event.message)
                    assertTrue(event.isLast)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `1 subscriber in 1 topic waiting for 1 message`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)

        val unsubscribe = brokerInstances.first().subscribe(
            topic = topic,
            handler = { event ->
                // Assert [1]
                assertEquals(topic, event.topic)
                assertEquals(FIRST_EVENT_ID, event.id)
                assertEquals(message, event.message)
                assertFalse(event.isLast)
                latch.countDown()
            }
        )

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `n subscribers in 1 topic waiting for 1 message with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        val latch = CountDownLatch(1)
        val unsubscribes = mutableListOf<() -> Unit>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    // Assert [1]
                    assertEquals(topic, event.topic)
                    assertEquals(FIRST_EVENT_ID, event.id)
                    assertEquals(message, event.message)
                    assertFalse(event.isLast)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Act
        brokerInstances.first().publish(
            topic = topic,
            message = message
        )

        // Assert [2]
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `1 subscriber in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_MESSAGES)
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        val unsubscribe = brokerInstances.first().subscribe(
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
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_MESSAGES, eventsReceived.size)
        eventsReceived.forEach { event ->
            assertEquals(topic, event.topic)
            assertContains(messages.indices, event.id.toInt())
            assertContains(messages, event.message)
            assertFalse(event.isLast)
        }

        /* eventsReceived.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        } */

        // Clean Up
        unsubscribe()
    }

    @Test
    fun `n subscribers in 1 topic receiving n messages with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
        }

        // Act
        messages.forEach { message ->
            getRandomBrokerInstance().publish(
                topic = topic,
                message = message
            )
        }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived.toSet()
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        eventsReceivedSet.forEach { event ->
            assertEquals(topic, event.topic)
            assertContains(messages.indices, event.id.toInt())
            assertContains(messages, event.message)
            assertFalse(event.isLast)
        }

        /* eventsReceivedSet.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        } */

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `n subscribers in n topics receiving n messages with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val latch = CountDownLatch(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        // Act
        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val unsubscribe = getRandomBrokerInstance().subscribe(
                    topic = entry.key,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
                unsubscribes.add(unsubscribe)
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
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_TOPICS * NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        eventsReceived.forEach { event ->
            val entry = topicsAndMessages[event.topic]
            assertNotNull(entry)
            assertContains(entry, event.message)
            assertFalse(event.isLast)
        }

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
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

        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)
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

        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)
    }

    @Test
    fun `should receive last published event on existing topic when new broker subscribes`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()

        var eventReceived = ""
        val latch = CountDownLatch(2)

        getRandomBrokerInstance().publish(topic, message)

        Thread.sleep(5000)

        // Act
        getRandomBrokerInstance().subscribe(topic) { event ->
            // Assert
            eventReceived = message
            assertEquals(topic, event.topic)
            assertEquals(FIRST_EVENT_ID, event.id)
            assertEquals(message, event.message)
            assertFalse(event.isLast)
            latch.countDown()
        }

        latch.await(1000, TimeUnit.MILLISECONDS)

        // Assert
        assertEquals(eventReceived, message)
    }

    @Test
    fun `n broker instance should successfully subscribe and receive past events from topics`() {
        // Arrange

        val topicsAndMessage = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to newRandomMessage()
        }
        topicsAndMessage.forEach {
            getRandomBrokerInstance().publish(it.key, it.value)
        }

        Thread.sleep(10000)

        // ACT
        val latch = CountDownLatch(topicsAndMessage.size)
        val receivedMessages = mutableListOf<String>()
        val unsubscribes = mutableListOf<() -> Unit>()
        topicsAndMessage.forEach { entry ->
            val un = getRandomBrokerInstance().subscribe(entry.key) { event ->
                // Assert
                if (event.topic == entry.key) {
                    receivedMessages.add(event.message)
                    latch.countDown()
                }
            }
            unsubscribes.add(un)
        }

        latch.await()

        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS))

        // Assert
        unsubscribes.forEach { it() }
        assertEquals(receivedMessages.toSet().toList().size, topicsAndMessage.map { it.value }.size)
        receivedMessages.toSet().toList().forEach {
            assertTrue(topicsAndMessage.map { pair -> pair.value }.contains(it))
        }
    }

    @Test
    fun `stress test with simultaneous publication of n messages to 1 topic with several broker instances involved`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = List(NUMBER_OF_MESSAGES) { newRandomMessage() }

        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES)
        val unsubscribes = mutableListOf<() -> Unit>()
        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()

        repeat(NUMBER_OF_SUBSCRIBERS) {
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    eventsReceived.add(event)
                    latch.countDown()
                }
            )
            unsubscribes.add(unsubscribe)
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
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertEquals(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES, eventsReceived.size)

        val eventsReceivedSet = eventsReceived
            .toSet()
            .map { event -> event.message }
        assertEquals(NUMBER_OF_MESSAGES, eventsReceivedSet.size)
        assertTrue(eventsReceivedSet.containsAll(messages))

        if (errors.isNotEmpty()) throw errors.peek()

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `stress test with simultaneous publication of n messages to n topics with several broker instances involved`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to List(NUMBER_OF_MESSAGES) { newRandomMessage() }
        }

        val threads = ConcurrentLinkedQueue<Thread>()
        val unsubscribes = mutableListOf<() -> Unit>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_SUBSCRIBERS * NUMBER_OF_MESSAGES * NUMBER_OF_TOPICS)

        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val unsubscribe = getRandomBrokerInstance().subscribe(
                    topic = entry.key,
                    handler = { event ->
                        eventsReceived.add(event)
                        latch.countDown()
                    }
                )
                unsubscribes.add(unsubscribe)
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
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

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

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `stress test with simultaneous subscription and publication of a message to n topics`() {
        // Arrange
        val topicsAndMessages = List(NUMBER_OF_TOPICS) { Pair(newRandomTopic(), newRandomMessage()) }

        val threads = ConcurrentLinkedQueue<Thread>()
        val unsubscribes = ConcurrentLinkedQueue<() -> Unit>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val eventsReceived = ConcurrentLinkedQueue<Event>()
        val latch = CountDownLatch(NUMBER_OF_TOPICS)

        // Act
        topicsAndMessages.forEach { pair ->
            val th = Thread {
                try {
                    val unsubscribe = getRandomBrokerInstance().subscribe(
                        topic = pair.first,
                        handler = { event ->
                            eventsReceived.add(event)
                            latch.countDown()
                        }
                    )
                    unsubscribes.add(unsubscribe)

                    getRandomBrokerInstance().publish(
                        topic = pair.first,
                        message = pair.second
                    )
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }
        threads.forEach { it.join() }

        // Assert
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        assertTrue(reachedZero)

        assertTrue(eventsReceived.size >= NUMBER_OF_TOPICS)

        topicsAndMessages.forEach { pair ->
            val event = eventsReceived.find { event -> event.topic == pair.first }
            assertNotNull(event)
            assertEquals(pair.second, event.message)
        }

        if (errors.isNotEmpty()) throw errors.peek()

        // Clean Up
        unsubscribes.forEach { unsubscribe -> unsubscribe() }
    }

    @Test
    fun `consecutive subscription and unSubscriptions while periodic publication of a message`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = ConcurrentLinkedQueue<String>()
        val lock = ReentrantLock()

        val publisherThread = Thread {
            while (!Thread.currentThread().isInterrupted) {
                lock.withLock {
                    newRandomMessage()
                        .also {
                            messages.add(it)
                        }
                        .also {
                            getRandomBrokerInstance().publish(
                                topic = topic,
                                message = it
                            )
                        }
                }
                Thread.sleep(PUBLISHER_DELAY_MILLIS)
            }
        }
        publisherThread.start()

        // Act
        val startTimeMillis = System.currentTimeMillis()
        while (true) {
            val events = ConcurrentLinkedQueue<Event>()
            val latch = CountDownLatch(2)
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    events.add(event)
                    latch.countDown()
                }
            )
            latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

            lock.withLock {
                assertTrue(events.map { it.message }.toSet().contains(messages.last()))
            }

            unsubscribe()

            val currentTimeMillis = System.currentTimeMillis()
            if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS) break
        }

        publisherThread.interrupt()
        publisherThread.join()
    }

    @Test
    fun `stress test with simultaneous subscription and unSubscriptions while periodic publication of a message`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = ConcurrentLinkedQueue<String>()
        val lock = ReentrantLock()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val threads = ConcurrentLinkedQueue<Thread>()

        val publisherThread = Thread {
            while (!Thread.currentThread().isInterrupted) {
                lock.withLock {
                    newRandomMessage()
                        .also { messages.offer(it) }
                        .also {
                            getRandomBrokerInstance().publish(
                                topic = topic,
                                message = it
                            )
                        }
                }
                Thread.sleep(PUBLISHER_DELAY_MILLIS)
            }
        }
        publisherThread.start()

        // Act
        val startTimeMillis = System.currentTimeMillis()
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val th = Thread {
                while (true) {
                    val events = ConcurrentLinkedQueue<Event>()
                    val latch = CountDownLatch(2)
                    val unsubscribe = getRandomBrokerInstance().subscribe(
                        topic = topic,
                        handler = { event ->
                            events.add(event)
                            latch.countDown()
                        }
                    )
                    try {
                        // Assert
                        latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

                        lock.withLock {
                            assertTrue(events.map { it.message }.toSet().contains(messages.last()))
                        }

                        unsubscribe()
                    } catch (e: AssertionError) {
                        failures.add(e)
                    } catch (e: Exception) {
                        errors.add(e)
                    }

                    val currentTimeMillis = System.currentTimeMillis()
                    if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS) break
                }
            }
            th.start().also { threads.add(th) }
        }

        threads.forEach { it.join() }
        publisherThread.interrupt()
        publisherThread.join()

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test with simultaneous subscription and unSubscriptions while periodic publication of a message in multiple topics`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to ConcurrentLinkedQueue<String>()
        }
        val lock = ReentrantLock()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val publisherThreads = ConcurrentLinkedQueue<Thread>()
        val threads = ConcurrentLinkedQueue<Thread>()

        topicsAndMessages.forEach { entry ->
            val publisherThread = Thread {
                while (!Thread.currentThread().isInterrupted) {
                    lock.withLock {
                        newRandomMessage()
                            .also { entry.value.offer(it) }
                            .also {
                                getRandomBrokerInstance().publish(
                                    topic = entry.key,
                                    message = it
                                )
                            }
                    }
                    Thread.sleep(PUBLISHER_DELAY_MILLIS)
                }
            }
            publisherThread.start().also { publisherThreads.add(publisherThread) }
        }

        // Act
        val startTimeMillis = System.currentTimeMillis()
        topicsAndMessages.forEach { entry ->
            repeat(NUMBER_OF_SUBSCRIBERS) {
                val th = Thread {
                    while (true) {
                        val events = ConcurrentLinkedQueue<Event>()
                        val latch = CountDownLatch(2)
                        val unsubscribe = getRandomBrokerInstance().subscribe(
                            topic = entry.key,
                            handler = { event ->
                                events.add(event)
                                latch.countDown()
                            }
                        )
                        try {
                            // Assert
                            latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

                            lock.withLock {
                                val messages = topicsAndMessages[entry.key]
                                assertNotNull(messages)
                                assertTrue(events.map { it.message }.toSet().contains(messages.last()))
                            }

                            unsubscribe()
                        } catch (e: AssertionError) {
                            failures.add(e)
                        } catch (e: Exception) {
                            errors.add(e)
                        }

                        val currentTimeMillis = System.currentTimeMillis()
                        if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS) break
                    }
                }
                th.start().also { threads.add(th) }
            }
        }

        threads.forEach { it.join() }
        publisherThreads.forEach { it.interrupt() }
        publisherThreads.forEach { it.join() }

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `consecutive subscription and unSubscriptions while periodic publication of a message and verify that all events are received in the correct order`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = ConcurrentLinkedQueue<String>()

        val publisherThread = Thread {
            while (!Thread.currentThread().isInterrupted) {
                newRandomMessage()
                    .also {
                        messages.add(it)
                    }
                    .also {
                        getRandomBrokerInstance().publish(
                            topic = topic,
                            message = it
                        )
                    }
                Thread.sleep(PUBLISHER_DELAY_MILLIS)
            }
        }
        publisherThread.start()

        // Act
        val events = ConcurrentLinkedQueue<Event>()
        val startTimeMillis = System.currentTimeMillis()
        while (true) {
            val latch = CountDownLatch(1)
            val unsubscribe = getRandomBrokerInstance().subscribe(
                topic = topic,
                handler = { event ->
                    events.add(event)
                    latch.countDown()
                }
            )
            val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
            assertTrue(reachedZero)

            unsubscribe()

            val currentTimeMillis = System.currentTimeMillis()
            if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= messages.size) {
                break
            }
        }

        publisherThread.interrupt()
        publisherThread.join()

        // Assert
        assertEquals(messages.toList(), events.map { it.message }.toSet().toList())
    }

    @Test
    fun `stress test with simultaneous subscription and unSubscriptions while periodic publication of a message and verify that all events are received in the correct order`() {
        // Arrange
        val topic = newRandomTopic()
        val messages = ConcurrentLinkedQueue<String>()

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val threads = ConcurrentLinkedQueue<Thread>()

        val publisherThread = Thread {
            while (!Thread.currentThread().isInterrupted) {
                newRandomMessage()
                    .also { messages.offer(it) }
                    .also {
                        getRandomBrokerInstance().publish(
                            topic = topic,
                            message = it
                        )
                    }
                Thread.sleep(PUBLISHER_DELAY_MILLIS)
            }
        }
        publisherThread.start()

        // Act
        val startTimeMillis = System.currentTimeMillis()
        repeat(NUMBER_OF_SUBSCRIBERS) {
            val th = Thread {
                try {
                    val events = ConcurrentLinkedQueue<Event>()
                    while (true) {
                        val latch = CountDownLatch(1)
                        val unsubscribe = getRandomBrokerInstance().subscribe(
                            topic = topic,
                            handler = { event ->
                                events.add(event)
                                latch.countDown()
                            }
                        )

                        // Assert [1]
                        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                        assertTrue(reachedZero)

                        unsubscribe()

                        val currentTimeMillis = System.currentTimeMillis()
                        if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= messages.size) {
                            break
                        }
                    }

                    // Assert [2]
                    assertEquals(messages.toList(), events.map { it.message }.toSet().toList())
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }

        threads.forEach { it.join() }
        publisherThread.interrupt()
        publisherThread.join()

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test with simultaneous subscription and unSubscriptions while periodic publication of a message in multiple topics and verify that all events are received in the correct order`() {
        // Arrange
        val topicsAndMessages = (1..NUMBER_OF_TOPICS).associate {
            newRandomTopic() to ConcurrentLinkedQueue<String>()
        }

        val failures = ConcurrentLinkedQueue<AssertionError>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val publisherThreads = ConcurrentLinkedQueue<Thread>()
        val threads = ConcurrentLinkedQueue<Thread>()

        topicsAndMessages.forEach { entry ->
            val publisherThread = Thread {
                while (!Thread.currentThread().isInterrupted) {
                    newRandomMessage()
                        .also {
                            entry.value.offer(it)
                        }
                        .also {
                            getRandomBrokerInstance().publish(
                                topic = entry.key,
                                message = it
                            )
                        }

                    Thread.sleep(PUBLISHER_DELAY_MILLIS)
                }
            }
            publisherThread.start().also { publisherThreads.add(publisherThread) }
        }

        val startTimeMillis = System.currentTimeMillis()
        topicsAndMessages.forEach { entry ->
            val th = Thread {
                try {
                    val events = ConcurrentLinkedQueue<Event>()
                    while (true) {
                        val latch = CountDownLatch(1)
                        val unsubscribe = getRandomBrokerInstance().subscribe(
                            topic = entry.key,
                            handler = { event ->
                                events.add(event)
                                latch.countDown()
                            }
                        )

                        // Assert [1]
                        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MINUTES)
                        assertTrue(reachedZero)

                        unsubscribe()
                        val currentTimeMillis = System.currentTimeMillis()
                        if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= entry.value.size) {
                            break
                        }
                    }

                    // Assert [2]
                    val originalList = topicsAndMessages[events.first().topic]?.toList()
                    val receivedList = events.map { it.message }.toSet().toList()
                    assertEquals(originalList, receivedList)
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }
        threads.forEach { it.join() }
        publisherThreads.forEach { it.interrupt() }
        publisherThreads.forEach { it.join() }

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `cannot invoke method shutdown twice`() {
        // Arrange
        val broker = createBrokerInstance()
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
        val broker = createBrokerInstance()
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
        val broker = createBrokerInstance()
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
        private const val NUMBER_OF_TOPICS = 5
        private const val NUMBER_OF_SUBSCRIBERS = 200
        private const val NUMBER_OF_MESSAGES = 200

        private const val PUBLISHER_DELAY_MILLIS = 3000L
        private const val SUBSCRIBE_TIMEOUT_MILLIS = 60000L
        private const val TEST_EXECUTION_TIME_MILLIS = 60000L

        private fun createBrokerInstance() =
            // - PostgreSQL
            BrokerSQL()

        // - Redis
        // BrokerRedis()

        // - RabbitMQ
        // BrokerRabbitStreams()

        private val brokerInstances = List(NUMBER_OF_BROKER_INSTANCES) { createBrokerInstance() }

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
