package cs4k.prototype.broker

/**
 * Represents a retry mechanism.
 * @param maxRetries the maximum number of retries.
 * @param waitTimeMillis the time to wait between retries.
 * @param message the message to throw when the maximum number of retries is reached.
 */
class Retry(
    private val maxRetries: Int = 3,
    private val waitTimeMillis: Long = 1000,
    private val message: String = "Maximum number of retries reached."
) {
    /**
     * Execute an action with retry mechanism.
     * @param action the action to execute.
     * @return the result of the action.
     */
    fun <T> executeWithRetry(action: () -> T): T {
        var result: T? = null
        var retryCount = 0

        while (result == null && retryCount < maxRetries) {
            try {
                result = action()
            } catch (e: Exception) {
                retryCount++
                Thread.sleep(waitTimeMillis)
            }
        }

        if (result == null) {
            throw IllegalStateException(message)
        }

        return result
    }
}