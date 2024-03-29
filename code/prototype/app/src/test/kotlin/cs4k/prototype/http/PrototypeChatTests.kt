package cs4k.prototype.http

import cs4k.prototype.utils.MessageTest
import org.junit.jupiter.api.Assertions.fail
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrototypeChatTests {

    @LocalServerPort
    var port: Int = 0

    @Test
    fun `new listener of a group receives the last message`() {
        // given: HTTP clients, a random group and a random message.
        val clientA = newClient(port)
        val clientB = newClient(port)
        val group = newRandomGroup()
        val message = MessageTest(newRandomMessage())

        // when: clientA send a message to group ...
        send(clientA, group, message)

        // ... and clientB listen group.
        val emitterB = listen(clientB, group)

        // then: clientB receives the message.
        val eventReceived = emitterB
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: fail("Message not received.")

        assertEquals(group, eventReceived.event())
        assertEquals(0L, eventReceived.id()?.toLong())
        assertTrue(eventReceived.data().toString().contains(message.message))
    }

    @Test
    fun `new listeners of a group receives the same last message `() {
        // given: HTTP clients, a random group and a random message.
        val clientA = newClient(port)
        val clients = List(NUMBER_OF_CLIENTS) { newClient(port) }
        val group = newRandomGroup()
        val message = MessageTest(newRandomMessage())

        // when: clientA send a message to group ...
        send(clientA, group, message)

        // ... and clients listen group.
        clients.forEach { client ->
            val emitter = listen(client, group)

            // then: clients receives the message.
            val eventReceived = emitter
                .take(1)
                .collectList()
                .block(Duration.ofSeconds(10))
                ?.first() ?: fail("Message not received.")

            assertEquals(group, eventReceived.event())
            assertEquals(0L, eventReceived.id()?.toLong())
            assertTrue(eventReceived.data().toString().contains(message.message))
        }
    }

    @Test
    fun `1 listener of a group receives 1 message`() {
        // given: HTTP clients, a random group and a random message ...
        val clientA = newClient(port)
        val clientB = newClient(port)
        val group = newRandomGroup()
        val message = MessageTest(newRandomMessage())

        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        val th = Thread {
            try {
                // when: clientB listen group ...
                val emitterB = listen(clientB, group)

                // then: clientB receives the message.
                val eventReceived = emitterB
                    .take(1)
                    .collectList()
                    .block(Duration.ofSeconds(10))
                    ?.first() ?: fail("Message not received.")

                assertEquals(group, eventReceived.event())
                assertEquals(0L, eventReceived.id()?.toLong())
                assertTrue(eventReceived.data().toString().contains(message.message))
            } catch (e: AssertionError) {
                failures.add(e)
            } catch (e: Exception) {
                errors.add(e)
            }
        }
        th.start()

        // ... and clientA send a message to group.
        send(clientA, group, message)

        th.join()

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `n listeners of a group receives 1 message`() {
        // given: HTTP clients, a random group and a random message ...
        val clientA = newClient(port)
        val clients = List(NUMBER_OF_CLIENTS) { newClient(port) }
        val group = newRandomGroup()
        val message = MessageTest(newRandomMessage())

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        // when: clients listen group ...
        clients.forEach { client ->
            val th = Thread {
                try {
                    val emitter = listen(client, group)

                    // then: clientB receives the message.
                    val eventReceived = emitter
                        .take(1)
                        .collectList()
                        .block(Duration.ofSeconds(10))
                        ?.first() ?: fail("Message not received.")

                    assertEquals(group, eventReceived.event())
                    assertEquals(0L, eventReceived.id()?.toLong())
                    assertTrue(eventReceived.data().toString().contains(message.message))
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }

        // ... and clientA send a message to group ...
        send(clientA, group, message)

        threads.forEach { it.join() }

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `1 listener of a group receives n message`() {
        // given: HTTP clients, a random group and a random messages ...
        val clientA = newClient(port)
        val group = newRandomGroup()
        val messages = List(NUMBER_OF_MESSAGES) { MessageTest(newRandomMessage()) }

        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        // when: clientA listen group ...
        val th = Thread {
            try {
                val emitterA = listen(clientA, group)

                // then: clientA receives messages.
                val eventsReceived = emitterA
                    .take(NUMBER_OF_MESSAGES.toLong())
                    .collectList()
                    .block(Duration.ofSeconds(10))
                    ?.toList() ?: fail("Message not received.")

                messages.forEachIndexed { idx, message ->
                    val event = eventsReceived.find { it.data()?.toString()?.contains(message.message) ?: false }
                    assertNotNull(event)
                    assertEquals(group, event.event())
                    assertEquals(idx.toLong(), event.id()?.toLong())
                }
            } catch (e: AssertionError) {
                failures.add(e)
            } catch (e: Exception) {
                errors.add(e)
            }
        }
        th.start()

        // ... and clients send messages to group.
        messages.forEach { message ->
            send(newClient(port), group, message)
        }

        th.join()

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `stress test n listener of a group receives n message`() {
        // given: HTTP clients, a random group and random messages ...
        val clients = List(NUMBER_OF_CLIENTS) { newClient(port) }
        val group = newRandomGroup()
        val messages = List(NUMBER_OF_MESSAGES) { MessageTest(newRandomMessage()) }

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        // when: clients listen group ...
        clients.forEach { client ->
            val th = Thread {
                try {
                    val emitter = listen(client, group)

                    // then: clients receives messages.
                    val eventsReceived = emitter
                        .take(NUMBER_OF_MESSAGES.toLong())
                        .collectList()
                        .block(Duration.ofSeconds(10))
                        ?.toList() ?: fail("Message not received.")

                    messages.forEachIndexed { idx, message ->
                        val event = eventsReceived.find { it.data()?.toString()?.contains(message.message) ?: false }
                        assertNotNull(event)
                        assertEquals(group, event.event())
                        assertEquals(idx.toLong(), event.id()?.toLong())
                    }
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }

        // ... and clients send messages to group.
        messages.forEach { message ->
            send(newClient(port), group, message)
        }

        threads.forEach { it.join() }

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    /*
    @Test
    fun `stress test n listener of n group receives n message`() {
        // given: HTTP clients, random groups and random messages ...
        val clients = List(NUMBER_OF_CLIENTS) { newClient(port) }
        val groupsAndMessages = (1..NUMBER_OF_GROUPS).associate {
            newRandomGroup() to List(NUMBER_OF_MESSAGES) { MessageTest(newRandomMessage()) }
        }

        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        // when: clients listen groups ...
        groupsAndMessages.forEach { entry ->
            clients.forEach { client ->
                val th = Thread {
                    try {
                        val emitter = listen(client, entry.key)

                        // then: clients receives messages.
                        val eventsReceived = emitter
                            .take(NUMBER_OF_MESSAGES.toLong())
                            .collectList()
                            .block(Duration.ofSeconds(10))
                            ?.toList() ?: fail("Message not received.")

                        groupsAndMessages[entry.key]?.forEach { message ->
                            val event = eventsReceived.find { it.data()?.toString()?.contains(message.message) ?: false }
                            assertNotNull(event)
                            assertEquals(entry.key, event.event())
                        }
                    } catch (e: AssertionError) {
                        failures.add(e)
                    } catch (e: Exception) {
                        errors.add(e)
                    }
                }
                th.start().also { threads.add(th) }
            }
        }

        // ... and clients send messages to group.
        groupsAndMessages.forEach { entry ->
            entry.value.forEach { message ->
                send(newClient(port), entry.key, message)
            }
        }

        threads.forEach { it.join() }

        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }
     */

    companion object {
        private const val NUMBER_OF_CLIENTS = 100

        // private const val NUMBER_OF_GROUPS = 10
        private const val NUMBER_OF_MESSAGES = 100

        private fun generateRandom() = abs(Random.nextLong())

        private fun newRandomGroup() = "group${generateRandom()}"
        private fun newRandomMessage() = "message${generateRandom()}"
        private fun newClient(port: Int) =
            WebTestClient.bindToServer().baseUrl("http://localhost:$port/api").build()

        private fun listen(client: WebTestClient, group: String) =
            client
                .get()
                .uri("/chat/listen?group=$group")
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.TEXT_EVENT_STREAM)
                .returnResult(ServerSentEvent::class.java)
                .responseBody

        private fun send(client: WebTestClient, group: String, message: MessageTest) {
            client
                .post()
                .uri("/chat/send?group=$group")
                .bodyValue(message)
                .exchange()
                .expectStatus().isOk
        }
    }
}
