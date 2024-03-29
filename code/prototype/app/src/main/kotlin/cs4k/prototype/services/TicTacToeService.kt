package cs4k.prototype.services

import cs4k.prototype.broker.Broker
import cs4k.prototype.domain.Game
import cs4k.prototype.domain.GameError
import cs4k.prototype.domain.GameInfo
import cs4k.prototype.repository.TicTacToeRepository
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.util.concurrent.TimeUnit

@Component
class TicTacToeService(
    val ticTacToeRepository: TicTacToeRepository,
    val broker: Broker
) {

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
        notifyGameState(id, newGame)
    }

    /**
     * Be notified of the game.
     * @param player the player part of the game.
     * @param id the id of the game.
     */
    fun relisten(player: String, id: Int): SseEmitter {
        val game = ticTacToeRepository.getGame(id)
        if (!game.isPlayerOfGame(player)) {
            throw GameError.NotYourGame()
        }
        return listenAndInitialNotify(id, game)
    }

    /**
     * Listen for changes in the game state and notify the player.
     * @param gameId the id of the game.
     * @param game the game to be played.
     */
    private fun listenAndInitialNotify(gameId: Int, game: Game): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))

        val unsubscribeCallback = broker.subscribe(
            topic = gameId.toString(),
            handler = { event ->
                val sseEmitterEvent = SseEmitter.event()
                    .name(event.topic)
                    .id(event.id.toString())
                    .data("event: ${event.topic} - id: ${event.id} - data: ${event.message}")
                sseEmitter.send(sseEmitterEvent)

                if (event.isLast) sseEmitter.complete()
            }
        )
        sseEmitter.onCompletion {
            unsubscribeCallback()
        }
        sseEmitter.onError {
            unsubscribeCallback()
        }

        notifyGameState(gameId, game)

        return sseEmitter
    }

    /**
     * Notify the player of the game state.
     * @param gameId the id of the game.
     * @param game the game to be played.
     */
    private fun notifyGameState(gameId: Int, game: Game) {
        val gameInfo = GameInfo(gameId, game)
        broker.publish(
            topic = "gameId$gameId",
            payload = gameInfo
        )
    }
}
