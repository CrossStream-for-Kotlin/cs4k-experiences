package cs4k.prototype.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

data class Game(
    val board: List<Piece> = emptyList(),
    @JsonProperty("xplayer")
    val xPlayer: String,
    @JsonProperty("oplayer")
    val oPlayer: String? = null,
    val gameSize: Int = 3
) {

    enum class Shape { X, O }
    enum class State { X_TURN, O_TURN, X_WIN, O_WIN, DRAW, WAITING }

    class Piece(
        val row: Int,
        val column: Int,
        val shape: Shape
    )

    private val state: State = when {
        oPlayer == null -> State.WAITING
        hasWon(Shape.X) -> State.X_WIN
        hasWon(Shape.O) -> State.O_WIN
        isDraw() -> State.DRAW
        board.count { it.shape == Shape.X } == board.count { it.shape == Shape.O } -> State.X_TURN
        board.count { it.shape == Shape.X } == board.count { it.shape == Shape.O } + 1 -> State.O_TURN
        else -> throw IllegalStateException("Cannot determine turn.")
    }

    private fun hasWon(shape: Shape): Boolean {
        val shapePieces = board.filter { it.shape == shape }
        return wonInRow(shapePieces) ||
                wonInColumn(shapePieces) ||
                wonInSlash(shapePieces) ||
                wonInBackSlash(shapePieces)
    }

    private fun isDraw() = board.count() == gameSize * gameSize

    private fun wonInRow(shapePieces: List<Piece>): Boolean {
        repeat(gameSize) { row ->
            if (shapePieces.count { it.row == row } >= gameSize) return true
        }
        return false
    }

    private fun wonInColumn(shapePieces: List<Piece>): Boolean {
        repeat(gameSize) { column ->
            if (shapePieces.count { it.column == column } >= gameSize) return true
        }
        return false
    }

    private fun wonInSlash(shapePieces: List<Piece>): Boolean {
        return shapePieces.count { it.row + it.column == gameSize - 1 } >= gameSize
    }

    private fun wonInBackSlash(shapePieces: List<Piece>): Boolean {
        return shapePieces.count { it.row == it.column } >= gameSize
    }

    private fun isPlayerOfGame(player: String): Boolean {
        return player == xPlayer || player == oPlayer
    }

    private fun isMyTurn(player: String): Boolean {
        return (player == xPlayer && state == State.X_TURN) || (player == oPlayer && state == State.O_TURN)
    }

    @JsonIgnore
    fun isOver(): Boolean = state != State.X_TURN && state != State.O_TURN && state != State.WAITING

    private fun isRowAndColumnValid(row: Int, column: Int): Boolean {
        return row in 0 until gameSize && column in 0 until gameSize
    }

    private fun currentShapePlayed() = when (state) {
        State.X_TURN -> Shape.X
        State.O_TURN -> Shape.O
        else -> throw IllegalStateException("Tried to play in illegal state.")
    }

    fun play(row: Int, column: Int, player: String): Game {
        if (!isPlayerOfGame(player)) throw GameError.NotYourGame()
        if (state == State.WAITING) throw GameError.HasNotStartedYet()
        if (isOver()) throw GameError.GameAlreadyOver()
        if (!isMyTurn(player)) throw GameError.NotYourTurn()
        if (!isRowAndColumnValid(row, column)) throw GameError.InvalidPosition()
        if (board.any { it.row == row && it.column == column }) throw GameError.PositionAlreadyOccupied()
        return copy(board = board + Piece(row, column, currentShapePlayed()))
    }
}
