package cs4k.prototype.broker.common

import org.slf4j.LoggerFactory

/**
 * Represents a retry mechanism.
 *
 * @param maxRetries The maximum number of retries.
 * @param waitTimeMillis The time to wait between retries.
 */
class RetryExecutor(
    private val maxRetries: Int = 3,
    private val waitTimeMillis: Long = 1000
) {

    /**
     * Execute an action with retry mechanism.
     *
     * @param action The action to execute.
     * @param exception The exception to throw if the action fails after retries.
     * @param retryCondition The condition to retry the action.
     * @return The result of the action.
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
                evaluateException(retryCondition, e)
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
     * @return The result of the action.
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
                evaluateException(retryCondition, e)
            }
        }
        throw exception()
    }

    /**
     * Evaluate exception.
     *
     * @param retryCondition The condition to retry the action.
     * @param exception The captured exception.
     */
    private fun evaluateException(retryCondition: (Throwable) -> Boolean, exception: Exception) {
        logger.error("error executing action, message '{}'", exception.message)
        if (retryCondition(exception)) {
            logger.error("... retrying ...")
            Thread.sleep(waitTimeMillis)
        } else {
            logger.error("... not retrying ...")
            throw exception
            // return
        }
    }

    private companion object {
        // Logger instance for logging Executor class error.
        private val logger = LoggerFactory.getLogger(RetryExecutor::class.java)
    }
}
