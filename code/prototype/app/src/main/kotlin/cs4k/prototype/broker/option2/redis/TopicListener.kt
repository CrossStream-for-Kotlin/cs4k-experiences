package cs4k.prototype.broker.option2.redis

import kotlinx.coroutines.Job

/**
 * Represents a stream topic listener.
 *
 * @property topic The stream topic.
 * @property startStreamMessageId The identifier of the last message read from stream topic.
 * @property job The job of the coroutine use to listen stream topic.
 */
data class TopicListener(
    val topic: String,
    val startStreamMessageId: String,
    val job: Job? = null
)
