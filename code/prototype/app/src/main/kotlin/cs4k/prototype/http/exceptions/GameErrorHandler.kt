package cs4k.prototype.http.exceptions

import cs4k.prototype.domain.GameError
import cs4k.prototype.http.models.output.ErrorResponse
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class GameErrorHandler {

    @ExceptionHandler(Exception::class)
    fun handler(exception: Exception): ResponseEntity<*> {
        return ErrorResponse.response(
            500,
            ErrorResponse.Type.INTERNAL_SERVER_ERROR,
            ErrorResponse.Title.INTERNAL_SERVER_ERROR
        )
    }

    @ExceptionHandler(GameError::class)
    fun handler(exception: GameError): ResponseEntity<*> {
        return when(exception) {
            is GameError.HasNotStartedYet -> ErrorResponse.response(
                400,
                ErrorResponse.Type.GAME_NOT_STARTED_YET,
                ErrorResponse.Title.GAME_NOT_STARTED_YET
            )

            is GameError.AlreadyWaiting -> ErrorResponse.response(
                400,
                ErrorResponse.Type.PLAYER_ALREADY_WAITING,
                ErrorResponse.Title.PLAYER_ALREADY_WAITING
            )

            is GameError.GameAlreadyOver -> ErrorResponse.response(
                400,
                ErrorResponse.Type.GAME_ALREADY_ENDED,
                ErrorResponse.Title.GAME_ALREADY_ENDED
            )

            is GameError.InvalidID -> ErrorResponse.response(
                400,
                ErrorResponse.Type.INVALID_ID,
                ErrorResponse.Title.INVALID_ID
            )

            is GameError.InvalidPosition -> ErrorResponse.response(
                400,
                ErrorResponse.Type.MOVE_NOT_VALID,
                ErrorResponse.Title.MOVE_NOT_VALID,
                ErrorResponse.Detail.MOVE_NOT_VALID
            )

            is GameError.NotYourGame -> ErrorResponse.response(
                401,
                ErrorResponse.Type.PLAYER_NOT_PART_OF_GAME,
                ErrorResponse.Title.PLAYER_NOT_PART_OF_GAME
            )

            is GameError.NotYourTurn -> ErrorResponse.response(
                400,
                ErrorResponse.Type.NOT_YOUR_TURN,
                ErrorResponse.Title.NOT_YOUR_TURN
            )

            is GameError.PositionAlreadyOccupied -> ErrorResponse.response(
                400,
                ErrorResponse.Type.MOVE_NOT_VALID,
                ErrorResponse.Title.MOVE_NOT_VALID,
                ErrorResponse.Detail.ALREADY_OCCUPIED
            )
        }
    }

}
