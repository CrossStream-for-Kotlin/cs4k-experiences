package cs4k.prototype.http.exceptions

import cs4k.prototype.domain.GameError
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler

@ControllerAdvice
class GameErrorHandler {

    // For demonstration and testing purposes only.
    @ExceptionHandler(GameError::class)
    fun handler(exception: GameError): ResponseEntity<*> {
        return ResponseEntity.status(400).body(exception::class.java)
    }
}
