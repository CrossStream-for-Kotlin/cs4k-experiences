package cs4k.prototype.domain

sealed class GameError : Exception() {

    class HasNotStartedYet : GameError()
    class InvalidID : GameError()
    class AlreadyWaiting : GameError()
    class NotYourGame : GameError()
    class NotYourTurn : GameError()
    class GameAlreadyOver : GameError()
    class InvalidPosition : GameError()
    class PositionAlreadyOccupied : GameError()
}
