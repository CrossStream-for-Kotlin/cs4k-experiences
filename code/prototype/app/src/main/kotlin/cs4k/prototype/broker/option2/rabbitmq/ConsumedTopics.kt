package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.client.Channel
import cs4k.prototype.broker.common.Event
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.coroutines.Continuation
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration

class ConsumedTopics {

    /**
     * Represents the information regarding an event.
     * @param id Sequential event ID.
     * @param payload The message of the event joined with the indicator if the message is the last one of the topic.
     */
    private class EventInfo(
        val id: Long,
        val payload: String
    ) {
        /**
         * Converting an event to the information of the event to reduce memory.
         * @param event The complete event.
         */
        constructor(event: Event) :
            this(event.id, listOf(event.message, event.isLast.toString()).joinToString(";"))

        /**
         * Converting the information of the event into an actual event to notify a subscriber.
         * @param topic The topic of the event.
         * @return The actual event.
         */
        fun toEvent(topic: String): Event {
            val splitPayload = payload.split(";")
            val message = splitPayload.dropLast(1).joinToString(";")
            val isLast = splitPayload.last().toBoolean()
            return Event(topic, id, message, isLast)
        }
    }

    /**
     * Information regarding consumption of a given topic.
     * @param channel Consuming channel.
     * @param lastOffset The offset related to the last event of the topic.
     * @param latestEvent The latest event read by the broker.
     * @param isBeingAnalyzed If the consumption of the topic is being analyzed for eventual cancelling.
     * @param lock Lock to control concurrent access to its consumption.
     */
    private data class ConsumeInfo(
        val channel: Channel? = null,
        val lastOffset: Long? = null,
        val latestEvent: EventInfo? = null,
        val isBeingAnalyzed: Boolean = false,
        val lock: Lock = ReentrantLock()
    )

    // Linking the topic to its consumption.
    private val topicToConsumeInfo = HashMap<String, ConsumeInfo>()

    // Locking to control concurrency.
    private val lock = ReentrantLock()

    /**
     * Structure of an offset request.
     * @param continuation Remainder of the code that is resumed when offset is obtained.
     * @param topic Topic that the one requesting wants the offset from.
     * @param offset The offset of the last event of the topic.
     */
    private class OffsetRequest(
        val continuation: Continuation<Unit>,
        val topic: String,
        var offset: Long? = null
    )

    // All requests.
    private val offsetRequestList = mutableListOf<OffsetRequest>()

    /**
     * Obtains all consumed events.
     * @return All consumed events.
     */
    fun getTopics() = lock.withLock {
        topicToConsumeInfo.keys
    }

    /**
     * Obtain a lock to control concurrently access to consumption of a topic.
     * @param topic Topic that is being consumed.
     * @returns The lock to control concurrent access.
     */
    fun getLock(topic: String): Lock? = lock.withLock {
        topicToConsumeInfo[topic]?.lock
    }

    /**
     * Checks if the topic is being consumed.
     * @param topic The topic needing to test.
     * @return true if it is being consumed, false otherwise.
     */
    fun isTopicBeingConsumed(topic: String): Boolean = lock.withLock {
        return topicToConsumeInfo[topic] != null
    }

    /**
     * Obtain a channel associated with topic.
     * @param topic The topic being consumed.
     * @return The channel where consumption is being done.
     */
    fun getChannel(topic: String) = lock.withLock {
        topicToConsumeInfo[topic]?.channel
    }

    /**
     * Associate a topic to a channel.
     * @param topic The topic being, or formerly was, consumed.
     * @param channel The channel where consumption is being done.
     */
    fun setChannel(topic: String, channel: Channel) = lock.withLock {
        topicToConsumeInfo[topic] = topicToConsumeInfo[topic]?.copy(channel = channel)
            ?: ConsumeInfo(channel = channel)
    }

    /**
     * Checks if the topic is being analyzed.
     * @param topic Topic whose consumer is being analyzed.
     * @return true if it is being currently analyzed, false if not or if the topic isn't being consumed.
     */
    fun isBeingAnalyzed(topic: String) = lock.withLock {
        topicToConsumeInfo[topic]?.isBeingAnalyzed ?: false
    }

    /**
     * Marks the channel as being used for analysis for eventual cleanup.
     * @param topic The topic being consumed.
     * @return true if it wasn't analyzed yet, and it was marked as such by the thread, false if it's already being
     * analyzed by another or if the topic in question is not being consumed.
     */
    fun markForAnalysis(topic: String) = lock.withLock {
        val info = topicToConsumeInfo[topic]
        if (info?.isBeingAnalyzed == false) {
            topicToConsumeInfo[topic] = info.copy(isBeingAnalyzed = true)
            true
        } else {
            false
        }
    }

    /**
     * Marks the channel as no longer being analyzed.
     * @param topic The topic being consumed.
     */
    fun stopAnalysis(topic: String) = lock.withLock {
        topicToConsumeInfo[topic]?.let { info -> topicToConsumeInfo[topic] = info.copy(isBeingAnalyzed = false) }
    }

    /**
     * Obtain the latest topic. When both sent and received are defined, received is prioritized.
     * @param topic The topic of the event desired.
     * @return The latest event of the topic.
     */
    fun getLatestEvent(topic: String): Event? = lock.withLock {
        topicToConsumeInfo[topic]?.latestEvent?.toEvent(topic)
    }

    /**
     * Create and set the latest event in a topic.
     * @param topic The topic of the event.
     * @param message The message of the event.
     * @param isLast If the event in question is the last of a given topic.
     * @return The newly created event.
     */
    fun createAndSetLatestEvent(topic: String, message: String, isLast: Boolean = false) = lock.withLock {
        val consumeInfo = topicToConsumeInfo[topic] ?: ConsumeInfo()
        val id = consumeInfo.latestEvent?.id?.plus(1L) ?: 0L
        val recentEvent = Event(topic, id, message, isLast)
        topicToConsumeInfo[topic] = consumeInfo.copy(latestEvent = EventInfo(recentEvent))
        recentEvent
    }

    /**
     * Create and set the latest event in a topic.
     * @param topic The topic of the event.
     * @param message The message of the event.
     * @param isLast If the event in question is the last of a given topic.
     * @return The newly created event.
     */
    fun createAndSetLatestEvent(topic: String, id: Long,  message: String, isLast: Boolean = false) = lock.withLock {
        val consumeInfo = topicToConsumeInfo[topic] ?: ConsumeInfo()
        val recentEvent = Event(topic, id, message, isLast)
        topicToConsumeInfo[topic] = consumeInfo.copy(latestEvent = EventInfo(recentEvent))
        recentEvent
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified.
     * @param topic The topic about to be consumed.
     * @param scope The scope of the running coroutine.
     * @param fetchOffset Suspend function that is able to externally fetch offset.
     * @return The latest offset available. Will return 0 if cancelled.
     */
    private suspend fun getOffset(topic: String, scope: CoroutineScope, fetchOffset: suspend (String) -> Long?): Long? {
        var myRequest: OffsetRequest? = null
        var offset: Long? = null
        var needsFetching = false
        try {
            suspendCancellableCoroutine<Unit> { continuation ->
                lock.withLock {
                    val memoryOffset = topicToConsumeInfo[topic]?.lastOffset
                    if (memoryOffset != null) {
                        offset = memoryOffset
                        continuation.resumeWith(Result.success(Unit))
                    } else {
                        myRequest = OffsetRequest(continuation, topic)
                        if (offsetRequestList.count { it.topic == topic } == 0) {
                            needsFetching = true
                        }
                        myRequest?.let { req ->
                            offsetRequestList.add(req)
                        }
                    }
                }
                if (needsFetching) {
                    scope.launch { fetchOffset(topic)?.let { setOffset(topic, it) } }
                }
            }
        } catch (e: CancellationException) {
            if (myRequest?.offset != null) {
                return requireNotNull(myRequest?.offset)
            } else {
                lock.withLock {
                    offsetRequestList.remove(myRequest)
                }
            }
            throw e
        }
        return offset ?: myRequest?.offset
    }

    /**
     * Reading the latest offset stored.
     * If there are no available offsets, then it will passively wait until notified or until timeout is reached.
     * @param topic The topic about to be consumed.
     * @param timeout Maximum amount of wait tine.
     * @return The latest offset, if able to be obtained.
     */
    fun getOffset(topic: String, timeout: Duration = Duration.INFINITE, fetchOffset: suspend (String) -> Long?): Long? {
        return runBlocking {
            var result: Long? = null
            try {
                withTimeout(timeout) {
                    result = getOffset(topic, this, fetchOffset)
                    result
                }
            } catch (e: CancellationException) {
                result
            }
        }
    }

    /**
     * Reading the latest offset stored without waiting.
     * @param topic The topic being consumed.
     */
    fun getOffsetNoWait(topic: String): Long? = lock.withLock {
        topicToConsumeInfo[topic]?.lastOffset
    }

    /**
     * Setting the latest offset. Anyone waiting will also be given it.
     * @param topic The topic being consumed.
     * @param offset The offset used in consumption of stream.
     */
    fun setOffset(topic: String, offset: Long) = lock.withLock { setOffsetUnlocked(topic, offset) }

    /**
     * Setting the latest offset. Anyone waiting will also be given it.
     * @param topic The topic being consumed.
     * @param offset The offset used in consumption of stream.
     */
    private fun setOffsetUnlocked(topic: String, offset: Long) {
        val storedOffset = topicToConsumeInfo[topic]?.lastOffset
        if (storedOffset == null || storedOffset < offset) {
            val consumeInfo = topicToConsumeInfo[topic]?.copy(lastOffset = offset)
                ?: ConsumeInfo(lastOffset = offset)
            topicToConsumeInfo[topic] = consumeInfo
            val requests = offsetRequestList.filter { it.topic == topic }
            requests.forEach {
                it.offset = offset
                it.continuation.resumeWith(Result.success(Unit))
            }
            offsetRequestList -= requests.toSet()
        }
    }

    /**
     * Removes all information related to the topic.
     * @param topic The topic no longer being consumed.
     */
    fun removeTopic(topic: String) = lock.withLock {
        topicToConsumeInfo.remove(topic)
        Unit
    }

    /**
     * Removing all information stored.
     */
    fun removeAll() = lock.withLock {
        topicToConsumeInfo.clear()
    }
}
