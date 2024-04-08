package cs4k.prototype.broker

import org.slf4j.LoggerFactory

/**
 * Represents a retry mechanism.
 * @param maxRetries the maximum number of retries.
 * @param waitTimeMillis the time to wait between retries.
 */
class RetryExecutor(
    private val maxRetries: Int = 3,
    private val waitTimeMillis: Long = 1000
) {
    /**
     * Execute an action with retry mechanism.
     * @param action the action to execute.
     * @param exception the exception to throw if the action fails.
     * @param retryCondition the condition to retry the action.
     * @return the result of the action.
     */
    fun <T> execute(
        exception:() -> BrokerException,
        action: () -> T,
        retryCondition: (Throwable) -> Boolean
        ): T {
        repeat(maxRetries) {
            try {
                return action()
            } catch (e: Exception) {
                if (!retryCondition(e)) {
                    throw e
                }
                Thread.sleep(waitTimeMillis)
                logger.error("Error executing action, retrying, {}", e.message)
            }
        }
        throw exception()
    }


    companion object {
        // Logger instance for logging Executor class error.
        private val logger = LoggerFactory.getLogger(RetryExecutor::class.java)

    }
}


