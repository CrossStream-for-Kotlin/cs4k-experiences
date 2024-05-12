package cs4k.prototype.broker.common

import org.slf4j.LoggerFactory

/**
 * Represents a retry mechanism.
 *
 * @param maxRetries The maximum number of retries.
 * @param waitTimeMillis The time to wait between retries.
 */
class RetryExecutor(
    private val maxRetries: Int = DEFAULT_MAX_RETRIES,
    private val waitTimeMillis: Long = DEFAULT_WAIT_TIME_MILLIS
) {

    /**
     * Execute an action with retry mechanism.
     *
     * @param action The action to execute.
     * @param exception The exception to throw if the action fails after retries.
     * @param retryCondition The condition to retry the action.
     * @return The result of the action or 'Unit' if not to repeat.
     * @throws BrokerException If the action cannot be executed after retries.
     */
    fun <T> execute(
        exception: () -> BrokerException,
        action: () -> T,
        retryCondition: (Throwable) -> Boolean = { true }
    ): T {
        repeat(maxRetries) {
            try {
                return action()
            } catch (e: Exception) {
                if (!isToRetry(retryCondition, e)) return Unit as T // throw exception
            }
        }
        throw exception()
    }

    /**
     * Execute a suspend action with retry mechanism.
     *
     * @param action The suspend action to execute.
     * @param exception The exception to throw if the action fails after retries.
     * @param retryCondition The condition to retry the action.
     * @return The result of the action or 'Unit' if not to repeat.
     * @throws BrokerException If the action cannot be executed after retries.
     */
    suspend fun <T> suspendExecute(
        exception: () -> BrokerException,
        action: suspend () -> T,
        retryCondition: (Throwable) -> Boolean = { true }
    ): T {
        repeat(maxRetries) {
            try {
                return action()
            } catch (e: Exception) {
                if (!isToRetry(retryCondition, e)) return Unit as T // throw exception
            }
        }
        throw exception()
    }

    /**
     * Evaluate exception.
     *
     * @param retryCondition The condition to retry the action.
     * @param exception The captured exception.
     * @return True if it is to retry.
     */
    private fun isToRetry(retryCondition: (Throwable) -> Boolean, exception: Exception): Boolean {
        logger.error("error executing action, message '{}'", exception.message)
        return if (retryCondition(exception)) {
            logger.error("... retrying ...")
            Thread.sleep(waitTimeMillis)
            true
        } else {
            logger.error("... not retrying ...")
            false
        }
    }

    private companion object {
        // Logger instance for logging Executor class error.
        private val logger = LoggerFactory.getLogger(RetryExecutor::class.java)

        // Default maximum number of retries.
        private const val DEFAULT_MAX_RETRIES = 3

        // Default time to wait between retries.
        private const val DEFAULT_WAIT_TIME_MILLIS = 1000L
    }
}
