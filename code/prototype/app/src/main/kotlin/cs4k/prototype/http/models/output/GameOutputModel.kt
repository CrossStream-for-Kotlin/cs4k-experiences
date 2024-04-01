package cs4k.prototype.http.models.output

import cs4k.prototype.domain.Game

data class GameOutputModel (
    val gameId: Int,
    val game: Game
)