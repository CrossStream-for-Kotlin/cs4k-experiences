package cs4k.prototype.http

import cs4k.prototype.services.ChatServiceV1
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
class ChatController(
    private val chatService: ChatServiceV1
) {

    @GetMapping(Uris.Chat.LISTEN)
    fun listen(): SseEmitter {
        val listener = chatService.newListener()
        return listener.sseEmitter
    }

    @GetMapping(Uris.Chat.LISTEN_GROUP)
    fun listenGroup(@PathVariable group: String): SseEmitter {
        val listener = chatService.newListener(group)
        return listener.sseEmitter
    }

    @PostMapping(Uris.Chat.SEND)
    fun send(@RequestBody message: String) {
        chatService.sendMessage(message)
    }

    @PostMapping(Uris.Chat.SEND_TO_GROUP)
    fun sendToGroup(@PathVariable group: String, @RequestBody message: String) {
        chatService.sendMessage(message, group)
    }
}
