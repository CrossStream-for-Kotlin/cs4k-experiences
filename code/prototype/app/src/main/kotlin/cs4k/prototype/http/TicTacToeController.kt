package cs4k.prototype.http

import cs4k.prototype.http.models.input.PlayInputModel
import cs4k.prototype.http.models.input.StartInputModel
import cs4k.prototype.services.TicTacToeService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
class TicTacToeController(
    private val ticTacToeService: TicTacToeService
) {

    @PostMapping(Uris.TicTacToe.START)
    fun start(@RequestBody startInputModel: StartInputModel): SseEmitter {
        return ticTacToeService.start(startInputModel.player)
    }

    @PostMapping(Uris.TicTacToe.PLAY)
    fun play(@RequestBody playInputModel: PlayInputModel, @PathVariable id: Int) {
        ticTacToeService.play(playInputModel.player, id, playInputModel.row, playInputModel.column)
    }

    @GetMapping(Uris.TicTacToe.WATCH)
    fun watch(@PathVariable id: Int): SseEmitter {
        return ticTacToeService.watch(id)
    }
}
