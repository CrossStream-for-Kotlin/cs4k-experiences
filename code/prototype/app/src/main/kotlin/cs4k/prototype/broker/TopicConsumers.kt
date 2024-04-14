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

    /**
     * Set topic with related info.
     * @param topic The topic.
     * @param value ConsumerTag associated with topic
     * @param offset Offset associated with consumption of messages of topic.
     */
    fun setTopic(topic: String, value: String, offset: Long) = lock.withLock {
        map[topic] = TopicInfo(value, offset)
    }

    /**
     * Set offset of the topic.
     * @param topic The topic.
     * @param offset Offset associated with consumption of messages of topic.
     */
    fun setTopicOffset(topic: String, offset: Long) = lock.withLock {
        val topicInfo = requireNotNull(map[topic]) { "Detected offset change with no topic." }
        map[topic] = topicInfo.copy(offset = offset)
    }

    /**
     * Removes consumerTag associated with topic.
     */
    fun removeConsumerTag(topic: String) = lock.withLock {
        val topicInfo = requireNotNull(map[topic]) { "Detected offset change with no topic." }
        map[topic] = topicInfo.copy(consumerTag = null)
    }

    /**
     * Iterates over every consumed topic and performs an action.
     */
    fun forEachConsumedTopic(action: (topic: String) -> Unit) = lock.withLock {
        map.forEach { (topic, info) ->
            if (info.isBeingConsumed()) {
                action(topic)
            }
        }
    }
}
