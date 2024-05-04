package cs4k.prototype.broker.option2.rabbitmq

import com.rabbitmq.stream.Producer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Container associating all topics with their respective producers.
 */
class TopicProducers {

    // Map that associates topics with producers
    private val map = HashMap<String, Producer>()

    // Lock to ensure thread safety.
    private val lock = ReentrantLock()

    /**
     * Obtain all topics that have producers making messages for them.
     */
    fun getAllTopics() = lock.withLock {
        map.keys.toList()
    }

    /**
     * Obtain the producer of a topic.
     * @param topic The topic
     * @return The producer of said topic.
     */
    fun getProducer(topic: String) = lock.withLock {
        map[topic]
    }

    /**
     * Define a producer of a topic.
     * @param topic The topic that has producers making messages for them
     * @param consumer The producer of said topic.
     *
     */
    fun setProducer(topic: String, consumer: Producer) = lock.withLock {
        map[topic] = consumer
    }

    /**
     * Removes producer and removes topic registry.
     * @param topic The topic no longer has messages to be sent.
     */
    fun removeProducer(topic: String) = lock.withLock {
        map.remove(topic)
    }
}
