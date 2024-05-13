package cs4k.prototype.services

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.option3.BrokerOption3DNS
import cs4k.prototype.broker.option3.BrokerOption3Multicast
import cs4k.prototype.domain.Game
import cs4k.prototype.domain.GameInfo
import cs4k.prototype.http.models.output.GameOutputModel
import cs4k.prototype.repository.TicTacToeRepository
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Component
class TicTacToeService(
    val ticTacToeRepository: TicTacToeRepository,
    val broker: BrokerOption3DNS
) {

    private val sseEmittersToKeepAlive = mutableListOf<SseEmitter>()
    private val lock = ReentrantLock()

    init {
        Executors.newScheduledThreadPool(1).also {
            it.scheduleAtFixedRate({ keepAlive() }, 2, 5, TimeUnit.SECONDS)
        }
    }

    /**
     * Start a new game or join an existing one.
     * @param player the player that wants to start a game.
     */
    fun start(player: String): SseEmitter {
        val otherPlayerRegister = ticTacToeRepository.getOtherPlayer(player)
        val gameId: Int
        val notifyingGame = if (otherPlayerRegister == null) {
            val game = Game(xPlayer = player)
            gameId = ticTacToeRepository.registerWaiting(game, player)
            game
        } else {
            gameId = otherPlayerRegister.gameId
            val game = ticTacToeRepository.getGame(gameId)
            val newGame = game.copy(oPlayer = player)
            ticTacToeRepository.startGame(newGame, gameId)
            newGame
        }
        return listenAndInitialNotify(gameId, notifyingGame)
    }

    /**
     * Play a move in a game.
     * @param player the player that wants to play.
     * @param id the id of the game.
     * @param row the row of the move.
     * @param column the column of the move.
     */
    fun play(player: String, id: Int, row: Int, column: Int) {
        val game = ticTacToeRepository.getGame(id)
        val newGame = game.play(row, column, player)
        ticTacToeRepository.updateGame(id, newGame)
        val gameInfo = GameInfo(id, newGame)
        notifyGameState(gameInfo)
    }

    /**
     * Be notified of the game.
     * @param id the id of the game.
     */
    fun watch(id: Int): SseEmitter {
        // Exception if there is no game with said id.
        ticTacToeRepository.getGame(id)
        return listenForGameStates(id)
    }

    /**
     * Listen for changes in the game state and notify the player.
     * @param gameId the id of the game.
     * @param game the game to be played.
     */
    private fun listenAndInitialNotify(gameId: Int, game: Game): SseEmitter {
        val sseEmitter = listenForGameStates(gameId)
        val gameInfo = GameInfo(gameId, game)
        notifyGameState(gameInfo)
        return sseEmitter
    }

    /**
     * Listen for changes in the game state.
     * @param gameId the id of the game.
     */
    private fun listenForGameStates(gameId: Int): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))
        lock.withLock { sseEmittersToKeepAlive.add(sseEmitter) }

        val unsubscribeCallback = broker.subscribe(
            topic = "gameId$gameId",
            handler = { event ->
                try {
                    val gameInfoReceived = deserializeJsonToGameInfo(event.message)
                    SseEvent.Message(
                        name = event.topic,
                        id = event.id,
                        data = GameOutputModel(gameInfoReceived.gameId, gameInfoReceived.game)
                    ).writeTo(
                        sseEmitter
                    )

                    if (event.isLast) sseEmitter.complete()
                } catch (ex: Exception) {
                    sseEmitter.completeWithError(ex)
                }
            }
        )
        sseEmitter.onCompletion {
            unsubscribeCallback()
            lock.withLock { sseEmittersToKeepAlive.remove(sseEmitter) }
        }
        sseEmitter.onError {
            unsubscribeCallback()
            lock.withLock { sseEmittersToKeepAlive.remove(sseEmitter) }
        }

        return sseEmitter
    }

    /**
     * Notify the player of the game state.
     * @param gameInfo The game information.
     */
    private fun notifyGameState(gameInfo: GameInfo) {
        broker.publish(
            topic = "gameId${gameInfo.gameId}",
            message = serializeGameInfoToJson(gameInfo),
            isLastMessage = gameInfo.game.isOver()
        )
    }

    /**
     * Send a keep alive to all active sseEmitters.
     */
    private fun keepAlive() = lock.withLock {
        val keepAlive = SseEvent.KeepAlive(Instant.now().epochSecond)
        sseEmittersToKeepAlive.forEach { sseEmitter ->
            try {
                keepAlive.writeTo(sseEmitter)
            } catch (ex: Exception) {
                // Ignore
            }
        }
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())

        private fun serializeGameInfoToJson(gameInfo: GameInfo) =
            objectMapper.writeValueAsString(gameInfo)

        private fun deserializeJsonToGameInfo(message: String) =
            objectMapper.readValue(message, GameInfo::class.java)
    }
}
