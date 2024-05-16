package cs4k.prototype.broker.option3

import kotlinx.coroutines.suspendCancellableCoroutine
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

suspend fun AsynchronousServerSocketChannel.acceptSuspend(): AsynchronousSocketChannel {
    return suspendCancellableCoroutine { continuation ->
        this.accept(
            null,
            object : CompletionHandler<AsynchronousSocketChannel?, Unit?> {
                override fun completed(result: AsynchronousSocketChannel?, attachment: Unit?) {
                    requireNotNull(result)
                    continuation.resume(result)
                }

                override fun failed(exc: Throwable?, attachment: Unit?) {
                    requireNotNull(exc)
                    continuation.resumeWithException(exc)
                }
            }
        )
    }
}
