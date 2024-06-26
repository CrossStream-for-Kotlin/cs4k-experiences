package cs4k.prototype.http

import cs4k.prototype.http.models.input.MessageInputModel
import cs4k.prototype.services.ChatService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
class ChatController(
    private val chatService: ChatService
) {

    @GetMapping(Uris.Chat.LISTEN)
    fun listen(@RequestParam group: String?): SseEmitter =
        chatService.newListener(group)

    @PostMapping(Uris.Chat.SEND)
    fun send(@RequestParam group: String?, @RequestBody body: MessageInputModel) {
        chatService.sendMessage(group, body.message)
    }
}
