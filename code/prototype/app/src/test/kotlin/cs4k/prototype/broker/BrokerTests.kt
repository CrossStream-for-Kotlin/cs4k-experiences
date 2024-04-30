package cs4k.prototype.broker

import cs4k.prototype.broker.BrokerException.BrokerTurnOffException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.RepeatedTest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
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
    /*
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
     */

    @Test
    fun `new broker instance receives events from existing topics`() {
        // Arrange
        val topic = newRandomTopic()
        val message = "teste"

        var eventReceived = ""
        val latch = CountDownLatch(2)

        val brokerInstance1 = BrokerRabbitQueues()
        brokerInstance1.publish(topic, message)

        // Espera um tempo para garantir que o evento seja pulicado
        Thread.sleep(5000)

        val brokerInstance2 = BrokerRabbitQueues()
        brokerInstance2.subscribe(topic) { event ->
            // Assert
            if (event.topic == topic && event.message == message) {
                eventReceived = message
                latch.countDown()
            }
        }

        // Act
        latch.await(10000, TimeUnit.MILLISECONDS)

        // Assert

        assertEquals(eventReceived, message)
    }


    @Test
    fun `new broker instance begginig in the middle of the exectution and asking for topics that are in the pass`(){
        // Arrange

        val brokerInstance1 = BrokerRabbitQueues()
        var list= mutableListOf<String>()
        var l2 = mutableListOf<String>()
        (1..100).forEach {
            val topic = newRandomTopic()
            val message = newRandomMessage()
            //ji want to add randomly picking a topic and adding to the list
            list.add(topic)
            l2.add(message)
            brokerInstance1.publish(topic, message)
        }


        // Espera um tempo para garantir que o evento seja pulicado
        Thread.sleep(5000)

        val brokerInstance2 = BrokerRabbitQueues()
        val eventReceived = AtomicBoolean(false)
        val latch = CountDownLatch(list.size)
        var l = mutableListOf<String>()
        list.forEach { topic ->
            brokerInstance2.subscribe(topic) { event ->
                // Assert
                if (event.topic == topic) {
                    l.add(event.message)
                    eventReceived.set(true)
                    latch.countDown()
                }
            }
        }

        // Assert

        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS))
        println("l2 =${l2.size}  l=${l.size}")
        assertEquals(l.toSet().toList(), l2)
        assertTrue(eventReceived.get())
    }

    /*
    @Test
    fun `new broker instance retrieves last event after significant delay`() {
        // Arrange
        val topic = newRandomTopic()
        val message = "persistent test message"
        val brokerInstance1 = getRandomBrokerInstance()

        // Primeira instância do broker publica um evento que deve ser persistido
        brokerInstance1.publish(topic, message)

        // Esperar um tempo significativo para simular o atraso na criação da nova instância
        Thread.sleep(5000)  // Representa um "longo tempo"

        // Criar uma nova instância do broker
        val brokerInstance2 = getRandomBrokerInstance()
        val eventReceived = AtomicBoolean(false)
        val latch = CountDownLatch(1)

        // Subscrever no tópico com a nova instância
        brokerInstance2.subscribe(topic) { event ->
            if (event.topic == topic && event.message == message) {
                eventReceived.set(true)
                latch.countDown()
            }
        }

        // Act
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

        // Assert
        assertTrue(reachedZero, "The event was not received within the expected time.")
        assertTrue(eventReceived.get(), "The event received does not match the expected event.")
    }


    @Test
    fun `new broker instance receives events from existing topics3`() {
        // Arrange
        val topic = newRandomTopic()
        val message = "teste"

        val topic1 = newRandomTopic()
        val message1 = "teste2"

        val eventReceived = AtomicBoolean(false)
        val latch = CountDownLatch(1)

        val brokerInstance1 = getRandomBrokerInstance()
        brokerInstance1.publish(topic, message)


        println("goiaba")
        val brokerInstance2 = getRandomBrokerInstance()
        brokerInstance2.subscribe(topic) { event ->
            // Assert
            if (event.topic == topic && event.message == message) {
                eventReceived.set(true)
            }
        }

        // Act
        val reachedZero = latch.await(SUBSCRIBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)

        // Assert
        assertTrue(reachedZero, "The event was not received within the expected time.")
        assertTrue(eventReceived.get(), "The event received does not match the expected event.")
    }


    @Test
    fun `new broker instance receives events from existing topics2`() {
        // Arrange
        val topic = newRandomTopic()
        val message = newRandomMessage()
        val latch = CountDownLatch(1)

        val list = mutableListOf<Pair<String, String>>()
        val brokerInstance1 = getRandomBrokerInstance()
        val listTopicEvents = (1..NUMBER_OF_MESSAGES).map { it ->
            val t = newRandomTopic()
            val m = newRandomMessage()
            list.add(Pair(t, m))
            brokerInstance1.publish(newRandomTopic(), newRandomMessage())
            if (it == NUMBER_OF_MESSAGES) {
                latch.countDown()
            }
        }


        val brokerInstance2 = getRandomBrokerInstance()


        // Act
    }

    @Test
    fun `new broker instance receives exactly the same events from existing topics`() {
        // Arrange
        val numberOfTopics = 5
        val latch = CountDownLatch(numberOfTopics)
        val receivedEvents = ConcurrentHashMap<String, Event>()

        val brokerInstance1 = getRandomBrokerInstance()
        val topicsAndMessages = (1..numberOfTopics).map {
            Pair(newRandomTopic(), newRandomMessage())
        }

        // Publicar eventos na primeira instância
        topicsAndMessages.forEach { (topic, message) ->
            brokerInstance1.publish(topic, message)
        }

        // Aguarde um pouco para garantir que os eventos sejam processados e publicados
        Thread.sleep(10000)

        // Segunda instância do broker
        val brokerInstance2 = getRandomBrokerInstance()

        // Inscrever no segundo broker para cada tópico
        topicsAndMessages.forEach { (topic, message) ->
            brokerInstance2.subscribe(topic) { event ->
                latch.countDown()
                if (event.message == message) {
                    receivedEvents.putIfAbsent(topic, event)
                }
            }
        }

        // Act
        val allEventsReceived = latch.await(5, TimeUnit.SECONDS)


        // Assert
        assertTrue(allEventsReceived, "Not all events were received within the timeout period.")
        assertEquals(numberOfTopics, receivedEvents.size, "Not all topics received the correct events.")

        // Verificar se os eventos recebidos são exatamente os mesmos que foram enviados
        topicsAndMessages.forEach { (topic, message) ->
            assertTrue(brokerInstance2.latestTopicEvents.showallEvents().map { it?.topic }.contains(topic))
            val receivedEvent = brokerInstance2.latestTopicEvents.showallEvents().find { it?.topic == topic }
            assertNotNull(receivedEvent, "Event not found for topic $topic.")
            assertEquals(message, receivedEvent.message)
        }
    }

    @Test
    fun `new broker instance receives exactly the same events from existing topics6`() {
        // Arrange
        val numberOfTopics = 100
        val latch = CountDownLatch(1)
        val receivedEvents = ConcurrentHashMap<String, Event>()
        val latch2 = CountDownLatch(numberOfTopics)
        val brokerInstance1 = getRandomBrokerInstance()
        val topicsAndMessages = (1..numberOfTopics).map {
            Pair(newRandomTopic(), newRandomMessage())
        }

        val topic2 = newRandomTopic()
        val message3 = newRandomMessage()
        brokerInstance1.publish(topic2, message3)


        topicsAndMessages.forEach { (topic, message) ->
            brokerInstance1.publish(topic, message)
            latch2.countDown()
        }

        latch2.await(5, TimeUnit.SECONDS)

        Thread.sleep(10000)

        println("Comecou depois de todos os publishers")

        val brokerInstance2 = BrokerRabbitQueues()


        brokerInstance2.subscribe(topic2) { event ->
            if (event.message == message3) {
                receivedEvents.putIfAbsent(topic2, event)
            }
            latch.countDown()
        }

        println("Eventos recebidos")
        brokerInstance2.latestTopicEvents.showallEvents().forEach() {
            println(it!!.topic)
            println(it!!.message)
        }
        println("Fim dos eventos recebidos")

        latch.await(15, TimeUnit.SECONDS)

        assertEquals(receivedEvents[topic2]!!.message, message3)
    }


    @Test
    fun `new broker instance retrieves last events for multiple topics`() {
        // Arrange
        val numberOfTopics = 5
        val numberOfEventsPerTopic = 3
        val topics = (1..numberOfTopics).map { newRandomTopic() }

        val brokerInstance1 = getRandomBrokerInstance()

        // Publica vários eventos em múltiplos tópicos
        topics.forEach { topic ->
            repeat(numberOfEventsPerTopic) { index ->
                val message = "Message $index for $topic"
                brokerInstance1.publish(topic, message, isLastMessage = (index == numberOfEventsPerTopic - 1))
            }
        }

        // Aguarde um pouco para garantir que os eventos foram processados e sincronizados
        Thread.sleep(2000)

        // Act
        val brokerInstance2 = getRandomBrokerInstance()
        val retrievedEvents = topics.associateWith { brokerInstance2.getLastEvent(it) }

        // Assert
        assertTrue(retrievedEvents.all { it.value != null }, "Not all topics have their last events retrieved.")
        assertTrue(
            retrievedEvents.all { (_, event) -> event!!.isLast },
            "Not all retrieved events are the last events for their topics."
        )

        // Additional check to ensure the content of the messages is correct
        topics.forEachIndexed { index, topic ->
            val expectedMessage = "Message ${numberOfEventsPerTopic - 1} for $topic"
            assertEquals(
                expectedMessage,
                retrievedEvents[topic]?.message,
                "Mismatch in message content for topic $topic."
            )
        }
    }
*/
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
        eventsReceived.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        }

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
        eventsReceivedSet.forEachIndexed { idx, event ->
            assertEquals(topic, event.topic)
            assertEquals(idx.toLong(), event.id)
            assertEquals(messages[idx], event.message)
            assertFalse(event.isLast)
        }

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
                            getRandomBrokerInstance().publish(
                                topic = topic,
                                message = it
                            )
                        }
                        .also {
                            messages.add(it)
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
                        .also {
                            getRandomBrokerInstance().publish(
                                topic = topic,
                                message = it
                            )
                        }
                        .also { messages.offer(it) }
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
                            .also {
                                getRandomBrokerInstance().publish(
                                    topic = entry.key,
                                    message = it
                                )
                            }
                            .also { entry.value.offer(it) }
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
                        getRandomBrokerInstance().publish(
                            topic = topic,
                            message = it
                        )
                    }
                    .also {
                        messages.add(it)
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
            if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= messages.size)
                break
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
                    .also {
                        getRandomBrokerInstance().publish(
                            topic = topic,
                            message = it
                        )
                    }
                    .also { messages.offer(it) }
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
                        if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= messages.size)
                            break
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
                            getRandomBrokerInstance().publish(
                                topic = entry.key,
                                message = it
                            )
                        }
                        .also {
                            entry.value.offer(it)
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
                        if (currentTimeMillis - startTimeMillis >= TEST_EXECUTION_TIME_MILLIS && events.size >= entry.value.size)
                            break
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
        //    Broker()

            // - Redis
            // BrokerRedisPubSubJedis()
            // BrokerRedisPubSubLettuce()
            // BrokerRedisStreams()

            // - RabbitMQ
            BrokerRabbitQueues()
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
