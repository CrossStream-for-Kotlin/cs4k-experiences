package cs4k.prototype.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.domain.Game
import cs4k.prototype.http.models.input.PlayInputModel
import cs4k.prototype.http.models.input.StartInputModel
import cs4k.prototype.http.models.output.GameOutputModel
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.math.abs
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PrototypeTTCTests {

    @LocalServerPort
    var port: Int = 0

    @Test
    fun `initial states of game are recorded`() {
        // given: 2 users
        val clientA = newClient(port)
        val clientB = newClient(port)
        val userA = newRandomUser()
        val userB = newRandomUser()

        // when: user A starts a game
        val emitterA = start(clientA, StartInputModel(userA))

        // then: user A gets an event, telling to wait
        val eventAReceived = emitterA
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameOutputModel::class.java)
        assertEquals(userA, gameAReceived.game.xPlayer)
        assertEquals(Game.State.WAITING, gameAReceived.game.state)

        // when: user B starts a game
        val emitterB = start(clientB, StartInputModel(userB))

        // then: user B gets two events, with the last being the new game with both players and initial state
        val eventBReceived = emitterB
            .take(2)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.last() ?: Assertions.fail("Message not received.")

        val gameBReceived = objectMapper.convertValue(eventBReceived.data(), GameOutputModel::class.java)
        assertEquals(userA, gameBReceived.game.xPlayer)
        assertEquals(userB, gameBReceived.game.oPlayer)
        assertEquals(Game.State.X_TURN, gameBReceived.game.state)
    }

    @Test
    fun `userA able to relisten`() {
        // given: 2 users
        val clientA = newClient(port)
        val userA = newRandomUser()
        val clientB = newClient(port)
        val userB = newRandomUser()

        // and: a game that has started
        val emitterA = start(clientA, StartInputModel(userA))
        start(clientB, StartInputModel(userB))

        val eventAReceived = emitterA
            .take(2)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.last() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameOutputModel::class.java)

        // when: user A goes to see the game
        val emitterA2 = watch(clientA, gameAReceived.gameId)

        // then: user A gets the latest state of the game, same as previously obtained.
        val eventAReceived2 = emitterA2
            .take(1)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.first() ?: Assertions.fail("Message not received.")

        val gameAReceived2 = objectMapper.convertValue(eventAReceived2.data(), GameOutputModel::class.java)
        assertEquals(gameAReceived, gameAReceived2)
    }

    @Test
    fun `n users are able to relisten`() {
        // given: 2 users
        val clientA = newClient(port)
        val userA = newRandomUser()
        val clientB = newClient(port)
        val userB = newRandomUser()

        // and: a game that has started
        val emitterA = start(clientA, StartInputModel(userA))
        start(clientB, StartInputModel(userB))

        val eventAReceived = emitterA
            .take(2)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.last() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameOutputModel::class.java)

        // and: multiple users
        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        val listeners = List(NUMBER_OF_USERS) { newClient(port) }

        listeners.forEach { client ->
            val th = Thread {
                try {
                    // when: each user wants to see the game
                    val emitter = watch(client, gameAReceived.gameId)

                    // then: the user will get the latest game state
                    val eventReceived = emitter
                        .take(1)
                        .collectList()
                        .block(Duration.ofMinutes(1))
                        ?.first() ?: Assertions.fail("Message not received.")

                    val gameReceived = objectMapper.convertValue(eventReceived.data(), GameOutputModel::class.java)
                    assertEquals(gameAReceived, gameReceived)
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }

        threads.forEach { it.join() }

        // checking for eventual failures.
        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    @Test
    fun `n users are able to see new plays`() {
        // given: 2 users
        val clientA = newClient(port)
        val userA = newRandomUser()
        val clientB = newClient(port)
        val userB = newRandomUser()

        // and: a play that is about to happen
        val playInputModel = PlayInputModel(userA, 1, 1)

        // and: a game that has started
        val emitterA = start(clientA, StartInputModel(userA))
        start(clientB, StartInputModel(userB))

        val eventAReceived = emitterA
            .take(2)
            .collectList()
            .block(Duration.ofSeconds(10))
            ?.last() ?: Assertions.fail("Message not received.")

        val gameAReceived = objectMapper.convertValue(eventAReceived.data(), GameOutputModel::class.java)

        // latch to synchronize the watch of all users to go before play
        val allThreadsAwaitingPlayLatch = CountDownLatch(NUMBER_OF_USERS)

        // and: multiple users
        val threads = ConcurrentLinkedQueue<Thread>()
        val errors = ConcurrentLinkedQueue<Exception>()
        val failures = ConcurrentLinkedQueue<AssertionError>()

        val listeners = List(NUMBER_OF_USERS) { newClient(port) }

        listeners.forEach { client ->
            val th = Thread {
                try {
                    // when: each user wants to see the game
                    val emitter = watch(client, gameAReceived.gameId)

                    // signaling test thread to make play
                    allThreadsAwaitingPlayLatch.countDown()

                    // then: the user will get the initial game state followed by game state with game
                    val eventReceived = emitter
                        .take(2)
                        .collectList()
                        .block(Duration.ofMinutes(1)) ?: Assertions.fail("Message not received.")

                    val initialBoard = objectMapper.convertValue(eventReceived[0].data(), GameOutputModel::class.java)
                    val currentBoard = objectMapper.convertValue(eventReceived[1].data(), GameOutputModel::class.java)

                    assertEquals(gameAReceived, initialBoard)
                    assertTrue(
                        currentBoard.game.board.all {
                                piece ->
                            piece.row == playInputModel.row &&
                                piece.column == playInputModel.column &&
                                piece.shape == Game.Shape.X
                        }
                    )

                    assertEquals(Game.State.O_TURN, currentBoard.game.state)
                } catch (e: AssertionError) {
                    failures.add(e)
                } catch (e: Exception) {
                    errors.add(e)
                }
            }
            th.start().also { threads.add(th) }
        }

        // waiting for all threads to make watch request to get both initial and new board with play
        allThreadsAwaitingPlayLatch.await()

        // and: user A makes a play
        play(clientA, gameAReceived.gameId, playInputModel)

        threads.forEach { it.join() }

        // checking for eventual failures.
        if (failures.isNotEmpty()) throw failures.peek()
        if (errors.isNotEmpty()) throw errors.peek()
    }

    companion object {

        private const val NUMBER_OF_USERS = 100

        private fun generateRandom() = abs(Random.nextLong() % 10000000)

        private fun newRandomUser() = "user${generateRandom()}"

        // ObjectMapper instance for serializing and deserializing JSON with Kotlin support.
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun newClient(port: Int) = WebTestClient.bindToServer().baseUrl("http://localhost:$port/api").build()

        private fun start(client: WebTestClient, body: StartInputModel) =
            client.post().uri("/game").bodyValue(body).exchange().expectStatus().isOk.expectHeader()
                .contentType(MediaType.TEXT_EVENT_STREAM).returnResult(ServerSentEvent::class.java).responseBody

        private fun play(client: WebTestClient, id: Int, body: PlayInputModel) {
            client.post().uri("/game/$id").bodyValue(body).exchange().expectStatus().isOk
        }

        private fun watch(client: WebTestClient, id: Int) =
            client.get().uri("/game/$id").exchange().expectStatus().isOk.expectHeader()
                .contentType(MediaType.TEXT_EVENT_STREAM).returnResult(ServerSentEvent::class.java).responseBody
    }
}
