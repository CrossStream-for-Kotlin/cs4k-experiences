package cs4k.prototype.services

import cs4k.prototype.broker.ListenerV1
import cs4k.prototype.domain.Game
import cs4k.prototype.repository.TicTacToeRepository
import org.springframework.stereotype.Component

@Component
class TicTacToeService(val ticTacToeRepository: TicTacToeRepository) {

    /**
     * Start a new game or join an existing one
     * @param player the player that wants to start a game
     */
    fun start(player: String): ListenerV1 {
        val otherPlayerRegister = ticTacToeRepository.getOtherPlayer(player)
        return if (otherPlayerRegister == null) {
            val game = Game(xPlayer = player)
            ticTacToeRepository.registerWaiting(game, player)
        } else {
            val game = ticTacToeRepository.getGame(otherPlayerRegister.gameId)
            val newGame = game.copy(oPlayer = player)
            ticTacToeRepository.startGame(newGame, otherPlayerRegister.gameId)
        }
    }

    /**
     * Play a move in a game
     * @param player the player that wants to play
     * @param id the id of the game
     * @param row the row of the move
     * @param column the column of the move
     */
    fun play(player: String, id: Int, row: Int, column: Int) {
        val game = ticTacToeRepository.getGame(id)
        val newGame = game.play(row, column, player)
        ticTacToeRepository.updateGame(id, newGame)
    }
}
