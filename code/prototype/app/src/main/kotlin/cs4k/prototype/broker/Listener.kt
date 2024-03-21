package cs4k.prototype.broker

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import javax.security.sasl.AuthorizeCallback

class Listener(
    val topic: String,
    val callback: (Event2, toComplete:Boolean) -> Unit
)