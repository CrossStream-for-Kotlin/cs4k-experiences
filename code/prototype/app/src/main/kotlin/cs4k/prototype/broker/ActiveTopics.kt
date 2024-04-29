package cs4k.prototype.broker

import kotlinx.coroutines.Job
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ActiveTopics {

    private val active = mutableListOf<Pair<String, Job?>>()

    private val lockActive = ReentrantLock()

    private val toActivate = mutableListOf<String>()

    private val lockToActivate = ReentrantLock()

    fun add(topic: String) {
        lockToActivate.withLock {
            toActivate.add(topic)
        }
    }

    fun get() =
        lockToActivate.withLock {
            toActivate.toList()
        }

    fun alter(topic: String, job: Job) {
        lockToActivate.withLock {
            toActivate.remove(topic)
        }
        lockActive.withLock {
            active.add(Pair(topic, job))
        }
    }

    fun remove(topic: String) {
        lockActive.withLock {
            active.find { it.first == topic }?.let {
                it.second?.cancel()
                active.remove(it)
            }
        }
    }
}
