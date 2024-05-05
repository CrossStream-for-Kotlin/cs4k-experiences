package cs4k.prototype.broker.option2.redis

import kotlinx.coroutines.Job
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Responsible for managing topics to listen to and topics being listening to.
 * It is thread-safe.
 */
class ListeningTopics {

    // Topics to listen to.
    private val toListen = mutableListOf<TopicListener>()

    // Lock to ensure thread safety in toListen manipulation.
    private val lockToListen = ReentrantLock()

    // Topics being listening to.
    private val listening = mutableListOf<TopicListener>()

    // Lock to ensure thread safety in listening manipulation.
    private val lockListening = ReentrantLock()

    /**
     * Add a topic to listen to.
     *
     * @param topic The stream topic.
     * @param startStreamMessageId The identifier of the last message read from stream topic.
     */
    fun add(topic: String, startStreamMessageId: String) {
        lockToListen.withLock {
            toListen.add(TopicListener(topic, startStreamMessageId))
        }
    }

    /**
     * Get all topics to listen to.
     *
     * @return The list of topics to listen to.
     */
    fun get() = lockToListen.withLock {
        toListen.toList()
    }

    /**
     * Remove topic from toListen and add to listening.
     *
     * @param topic The stream topic.
     * @param startStreamMessageId The identifier of the last message read from stream topic.
     * @param job The job of the coroutine use to listen stream topic.
     */
    fun alter(topic: String, startStreamMessageId: String, job: Job) {
        lockToListen.withLock {
            lockListening.withLock {
                toListen.find { it.topic == topic }?.let {
                    toListen.remove(it)
                    listening.add(TopicListener(topic, startStreamMessageId, job))
                }
            }
        }
    }

    /**
     * Remove topic from listening and cancel job to stop coroutine.
     *
     * @param topic The stream topic.
     */
    fun remove(topic: String) {
        lockListening.withLock {
            listening.find { it.topic == topic }?.let {
                it.job?.cancel()
                listening.remove(it)
            }
        }
    }
}
