package cs4k.prototype.broker

/**
 * Represents a retry mechanism.
 * @param maxRetries the maximum number of retries.
 * @param waitTimeMillis the time to wait between retries.
 */
class Executor(
    private val maxRetries: Int = 3,
    private val waitTimeMillis: Long = 1000
) {
    /**
     * Execute an action with retry mechanism.
     * @param action the action to execute.
     * @return the result of the action.
     */
    fun <T> executeWithRetry(
        exceptionMessage: String = "Failed to execute action with retry mechanism.",
        action: () -> T
    ): T {
        repeat(maxRetries) {
            try {
                return action()
            } catch (e: Exception) {
                Thread.sleep(waitTimeMillis)
            }
        }
        throw Exception(exceptionMessage)
    }
}
