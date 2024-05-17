package cs4k.prototype.broker.option3

import kotlinx.coroutines.suspendCancellableCoroutine
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Suspend function that accepts an inbound connection.
 */
suspend fun AsynchronousServerSocketChannel.acceptSuspend(): AsynchronousSocketChannel {
    return suspendCancellableCoroutine { continuation ->
        this.accept(
            null,
            object : CompletionHandler<AsynchronousSocketChannel?, Unit?> {
                override fun completed(result: AsynchronousSocketChannel?, attachment: Unit?) {
                    requireNotNull(result) { "The 'result' should not be null." }
                    continuation.resume(result)
                }

                override fun failed(exc: Throwable?, attachment: Unit?) {
                    requireNotNull(exc) { "The 'exc' should not be null." }
                    continuation.resumeWithException(exc)
                }
            }
        )
    }
}
