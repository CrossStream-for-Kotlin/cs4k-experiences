package cs4k.prototype.broker

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.math.max
import kotlin.time.Duration

/**
 * Thread-safe storage of the latest events.
 */
class LatestEventStore {

    // Map that associates topics with their respective recent events.
    private val map = HashMap<String, Event>()

    // Represents a person wanting the most recent event.
    private class EventRequest(
        val topic: String,
        val continuation: Continuation<Unit>,
        var event: Event? = null
    )

    // Wait list where waiting
    private val eventWaitList = mutableListOf<EventRequest>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtaining the most recent event - passively waiting if there isn't any event yet.
     */
    private suspend fun getLatestEvent(topic: String): Event {
        var myRequest: EventRequest? = null
        var lastEvent: Event? = null
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    val event = map[topic]
                    if (event != null) {
                        lastEvent = event
                        continuation.resumeWith(Result.success(Unit))
                    } else {
                        myRequest = EventRequest(topic, continuation)
                        myRequest?.let { req ->
                            eventWaitList.add(req)
                        }
                    }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.event != null) {
                return requireNotNull(myRequest?.event)
            } else {
                lock.withLock {
                    eventWaitList.remove(myRequest)
                }
                throw e
            }
        }
        return lastEvent ?: myRequest?.event!!
    }

    /**
     * Obtaining the most recent event - passively waiting if there isn't any event yet.
     * If timeout is reached and there's no event registered, it will return null.
     */
    fun getLatestEvent(topic: String, timeout: Duration = Duration.INFINITE): Event? {
        return runBlocking {
            var result: Event? = null
            try {
                withTimeout(timeout) {
                    result = getLatestEvent(topic)
                    result
                }
            } catch (e: CancellationException) {
                result
            }
        }
    }

    /**
     * Set the latest event in a topic.
     */
    fun setLatestEvent(topic: String, event: Event) = lock.withLock {
        val storedEvent = map[topic]
        val id = if (storedEvent == null) event.id else max(storedEvent.id, event.id) + 1
        val recentEvent = event.copy(id = id)
        map[topic] = recentEvent
        val requests = eventWaitList.filter { it.topic == topic }
        requests.forEach {
            it.event = recentEvent
            it.continuation.resumeWith(Result.success(Unit))
        }
        eventWaitList -= requests.toSet()
        id
    }

    /**
     * Removes the event of a given topic.
     */
    fun removeLatestEvent(topic: String) = lock.withLock {
        map.remove(topic)
    }
}
