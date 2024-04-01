package cs4k.prototype.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.domain.Game
import cs4k.prototype.domain.GameInfo
import cs4k.prototype.http.models.input.PlayInputModel
import cs4k.prototype.http.models.input.StartInputModel
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.Duration
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertEquals

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrototypeTTCTests {

    @LocalServerPort
    var port: Int = 0

    @Test
    fun `initial states of game are recorded`() {
        val clientA = newClient(port)
        val clientB = newClient(port)
        val userA = newRandomUser()
        val userB = newRandomUser()

        val emitterA = start(clientA, StartInputModel(userA))

        val eventAReceived = emitterA
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameInfo::class.java)

        assertEquals(userA, gameAReceived.game.xPlayer)
        assertEquals(Game.State.WAITING, gameAReceived.game.state)

        val emitterB = start(clientB, StartInputModel(userB))

        val eventBReceived = emitterB
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameBReceived = objectMapper.convertValue(eventBReceived.data(), GameInfo::class.java)

        assertEquals(userA, gameBReceived.game.xPlayer)
        assertEquals(userB, gameBReceived.game.oPlayer)
        assertEquals(Game.State.X_TURN, gameBReceived.game.state)
    }

    @Test
    fun `userA able to relisten`() {
        val clientA = newClient(port)
        val userA = newRandomUser()
        val clientB = newClient(port)
        val userB = newRandomUser()

        start(clientB, StartInputModel(userB))
        val emitterA = start(clientA, StartInputModel(userA))

        val eventAReceived = emitterA
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameInfo::class.java)

        val emitterA2 = relisten(clientA, gameAReceived.gameId)

        val eventAReceived2 = emitterA2
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameAReceived2 = objectMapper.convertValue(eventAReceived2.data(), GameInfo::class.java)

        assertEquals(gameAReceived, gameAReceived2)
    }

    companion object {
        private const val NUMBER_OF_USERS = 1000

        private fun generateRandom() = abs(Random.nextLong() % 10000000)

        private fun newRandomUser() = "user${generateRandom()}"

        // ObjectMapper instance for serializing and deserializing JSON with Kotlin support.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun newClient(port: Int) =
            WebTestClient.bindToServer().baseUrl("http://localhost:$port/api").build()

        private fun start(client: WebTestClient, body: StartInputModel) =
            client
                .post()
                .uri("/game")
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.TEXT_EVENT_STREAM)
                .returnResult(ServerSentEvent::class.java)
                .responseBody

        private fun play(client: WebTestClient, id: Int, body: PlayInputModel) {
            client
                .post()
                .uri("/game/$id")
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk
        }

        private fun relisten(client: WebTestClient, id: Int) =
            client
                .get()
                .uri("/game/$id")
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentType(MediaType.TEXT_EVENT_STREAM)
                .returnResult(ServerSentEvent::class.java)
                .responseBody
    }
}
