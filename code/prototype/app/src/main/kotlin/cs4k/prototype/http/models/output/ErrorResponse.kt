package cs4k.prototype.http.models.output

import com.fasterxml.jackson.annotation.JsonInclude
import org.springframework.http.ResponseEntity
import java.net.URI

@JsonInclude(JsonInclude.Include.NON_NULL)
class ErrorResponse(
    val type: URI,
    val title: String? = null,
    val detail: String? = null,
    val instance: URI? = null
) {

    companion object {

        const val DOC_LOCATION = "https://cs4k-demo.com/errors/"
        private const val MEDIA_TYPE = "application/problem+json"

        fun response(
            status: Int,
            type: URI,
            title: String? = null,
            detail: String? = null,
            instance: URI? = null
        ): ResponseEntity<Any> {
            return ResponseEntity
                .status(status)
                .header("Content-Type", MEDIA_TYPE)
                .body(ErrorResponse(type, title, detail, instance))
        }
    }

    object Type {

        val INTERNAL_SERVER_ERROR = URI(DOC_LOCATION + "internalServerError")
        val INVALID_ID = URI(DOC_LOCATION + "invalidId")
        val PLAYER_ALREADY_WAITING = URI(DOC_LOCATION + "playerAlreadyWaiting")
        val GAME_NOT_STARTED_YET = URI(DOC_LOCATION + "gameNotStartedYet")
        val PLAYER_NOT_PART_OF_GAME = URI(DOC_LOCATION + "playerNotPartOfGame")
        val MOVE_NOT_VALID = URI(DOC_LOCATION + "moveNotValid")
        val GAME_ALREADY_ENDED = URI(DOC_LOCATION + "gameAlreadyEnded")
        val NOT_YOUR_TURN = URI(DOC_LOCATION + "notYourTurn")
    }

    object Title {
        const val INTERNAL_SERVER_ERROR = "Unknown error occurred."
        const val INVALID_ID = "The ID provided does not exist."
        const val GAME_NOT_STARTED_YET = "Game has not started yet."
        const val PLAYER_NOT_PART_OF_GAME = "You are not a player in this game."
        const val MOVE_NOT_VALID = "Move invalid."
        const val GAME_ALREADY_ENDED = "Game ended."
        const val NOT_YOUR_TURN = "It is not your turn yet."
        const val PLAYER_ALREADY_WAITING = "You are already waiting."
    }

    object Detail {
        const val MOVE_NOT_VALID = "The position given is invalid."
        const val ALREADY_OCCUPIED = "The position given is already occupied."
    }
}
