package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.stream.Consumer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Container associating all topics with their respective consumers.
 */
class TopicConsumers {

    // Map that associates topics with consumers
    private val map = HashMap<String, Consumer>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtain all topics that are being consumed.
     */
    fun getAllTopics() = lock.withLock {
        map.keys.toList()
    }

    /**
     * Obtain the consumer of a topic.
     * @param topic The topic of consumption.
     * @return The consumer of said topic.
     */
    fun getConsumer(topic: String) = lock.withLock {
        map[topic]
    }

    /**
     * Define a consumer of a topic.
     * @param topic The topic that is being consumed.
     * @param consumer The consumer of said topic.
     *
     */
    fun setConsumer(topic: String, consumer: Consumer) = lock.withLock {
        map[topic] = consumer
    }

    /**
     * Removes consumer and removes topic registry.
     * @param topic The topic no longer being consumed.
     */
    fun removeConsumer(topic: String) = lock.withLock {
        map.remove(topic)
    }
}
