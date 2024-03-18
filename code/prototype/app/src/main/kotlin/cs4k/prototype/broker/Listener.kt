package cs4k.prototype.broker

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

class Listener(
    val channel: String,
    val sseEmitter: SseEmitter
)