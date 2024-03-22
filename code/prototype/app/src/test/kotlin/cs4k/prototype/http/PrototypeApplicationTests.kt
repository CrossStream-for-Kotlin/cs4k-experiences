package cs4k.prototype.http

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrototypeApplicationTests {

    @LocalServerPort
    var port: Int = 0

    @Test
    fun `chat test`() {
        // given: two HTTP client
        val clientA = WebTestClient.bindToServer().baseUrl("http://localhost:$port").build()
        val clientB = WebTestClient.bindToServer().baseUrl("http://localhost:$port").build()

        // when: clientA listen general group ...
        val sseA = clientA
            .get()
            .uri("/api/chat/listen")
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
            .returnResult(ServerSentEvent::class.java)
            .responseBody

        // when: ... and clientB listen general group ...
        val sseB = clientB
            .get()
            .uri("/api/chat/listen")
            .exchange()
            .expectStatus().isOk
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
            .returnResult(ServerSentEvent::class.java)
            .responseBody

        // when: ... and clientA send messages to general group.
        val messagesToSent = (0 until 1).toSet()
        messagesToSent.forEach { message ->
            clientA
                .post()
                .uri("/api/chat/send")
                .bodyValue(message.toString())
                .exchange()
                .expectStatus().isOk
        }

        // then: clientA receives all messages ...
        val messagesReceivedSSEA = sseA.take(messagesToSent.size.toLong())
            .map { it.data() }
            .collectList()
            .block(Duration.ofSeconds(10))

        assertNotNull(messagesReceivedSSEA)
        assertEquals(messagesToSent.size, messagesReceivedSSEA?.size)
        messagesReceivedSSEA?.containsAll(messagesToSent.map { it.toString() })?.let { assertTrue(it) }

        // then: ... and clientB receives all messages ...
        val messagesReceivedSSEB = sseB.take(messagesToSent.size.toLong())
            .map { it.data() }
            .collectList()
            .block(Duration.ofSeconds(20))

        assertNotNull(messagesReceivedSSEB)
        assertEquals(messagesToSent.size, messagesReceivedSSEB?.size)
        messagesReceivedSSEB?.containsAll(messagesToSent.map { it.toString() })?.let { assertTrue(it) }
    }
}
