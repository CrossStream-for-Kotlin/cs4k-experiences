package cs4k.prototype.broker.option2.experiences

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.time.Duration

/**
 * Thread safe offset storage, used to consume from streams.
 */
class LatestOffsetFetcher(
    val fetchAction: (String) -> Long
) {

    // Executor used to externally fetch an offset.
    private val fetchingExecutor = Executors.newSingleThreadExecutor()

    /**
     * Fetch an offset from an external source and then stores it.
     * @param topic The topic being consumed.
     */
    private fun fetchOffset(topic: String) {
        val offset = fetchAction(topic)
        setOffset(topic, offset)
    }

    // Map joining a topic with its respective latest offset.
    private val offsets = HashMap<String, Long>()

    // Lock to control concurrency.
    private val lock = ReentrantLock()

    // Structure of a request for a given offset.
    private class OffsetRequest(
        val topic: String,
        val continuation: Continuation<Unit>,
        var offset: Long? = null
    )

    // List of all the requests.
    private val waitList = mutableListOf<OffsetRequest>()

    /**
     * Setting the latest offset. Anyone waiting will also be given it.
     * @param topic The topic being consumed.
     * @param offset The offset used in consumption of stream.
     */
    fun setOffset(topic: String, offset: Long) = lock.withLock {
        val storedOffset = offsets[topic]
        if (storedOffset == null || storedOffset < offset) {
            offsets[topic] = offset
            val requests = waitList.filter { it.topic == topic }
            requests.forEach {
                it.offset = offset
                it.continuation.resumeWith(Result.success(Unit))
            }
            waitList -= requests.toSet()
        }
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified.
     * @param topic The topic about to be consumed.
     */
    private suspend fun getOffset(topic: String): Long {
        var myRequest: OffsetRequest? = null
        var offset: Long? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    val memoryOffset = offsets[topic]
                    if (memoryOffset != null) {
                        offset = memoryOffset
                        continuation.resumeWith(Result.success(Unit))
                    } else {
                        myRequest = OffsetRequest(topic, continuation)
                        myRequest?.let { req ->
                            waitList.add(req)
                        }
                        if (waitList.count { it.topic == topic } == 1) fetchingExecutor.execute { fetchOffset(topic) }
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.offset != null) {
                return requireNotNull(myRequest?.offset)
            } else {
                lock.withLock {
                    waitList.remove(myRequest)
                }
            }
            throw e
        }
        return offset ?: myRequest?.offset ?: 0L
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified or until timeout is reached.
     * @param topic The topic about to be consumed.
     * @param timeout Maximum amount of wait tine.
     */
    fun getOffset(topic: String, timeout: Duration = Duration.INFINITE): Long {
        return runBlocking {
            var result: Long? = null
            try {
                withTimeout(timeout) {
                    result = getOffset(topic)
                    result ?: 0L
                }
            } catch (e: CancellationException) {
                result ?: 0L
            }
        }
    }

    /**
     * Reading the latest offset stored without waiting.
     * @param topic The topic being consumed.
     */
    fun getOffsetNoWait(topic: String): Long? = lock.withLock {
        offsets[topic]
    }

    /**
     * Letting go of resources used to store the offset.
     * @param topic The topic formerly being consumed.
     */
    fun removeOffset(topic: String) = lock.withLock { offsets.remove(topic) }

    /**
     * Shuts down the internal executor.
     */
    fun shutdown() {
        fetchingExecutor.shutdown()
    }
}
