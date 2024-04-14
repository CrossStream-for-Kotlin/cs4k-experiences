package cs4k.prototype.broker

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class TopicConsumers {

    private data class TopicInfo(
        val consumerTag: String?,
        val offset: Long
    ) {

        fun isBeingConsumed() = consumerTag == null
    }

    // Map that associates topics with lists of subscribers.
    private val map = HashMap<String, TopicInfo>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Get consumerTag associated with a topic.
     *
     * @param topic The topic to get the subscribers from.
     * @return The consumerTag associated with the topic.
     */
    fun getConsumerTag(topic: String) = lock.withLock {
        map[topic]?.consumerTag
    }

    /**
     * Get offset associated with a topic.
     *
     * @param topic The topic to get the subscribers from.
     * @return The offset associated with the topic.
     */
    fun getOffset(topic: String) = lock.withLock {
        map[topic]?.offset
    }

    fun setTopic(topic: String, value: String, offset: Long) = lock.withLock {
        map[topic] = TopicInfo(value, offset)
    }

    fun setTopicOffset(topic: String, offset: Long) = lock.withLock {
        val topicInfo = requireNotNull(map[topic]) { "Detected offset change with no topic." }
        map[topic] = topicInfo.copy(offset = offset)
    }

    fun removeConsumerTag(topic: String) = lock.withLock {
        val topicInfo = requireNotNull(map[topic]) { "Detected offset change with no topic." }
        map[topic] = topicInfo.copy(consumerTag = null)
    }

    fun forEachConsumedTopic(action: (topic: String) -> Unit) = lock.withLock {
        map.forEach { (topic, info) ->
            if (info.isBeingConsumed()) {
                action(topic)
            }
        }
    }
}
