package cs4k.prototype.http

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
    fun listen(@RequestParam group: String?): SseEmitter {
        return chatService.newListener(group)
    }

    @PostMapping(Uris.Chat.SEND)
    fun send(@RequestParam group: String?, @RequestBody message: String) {
        chatService.sendMessage(group, message)
    }
}
