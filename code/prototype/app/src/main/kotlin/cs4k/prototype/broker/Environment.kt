package cs4k.prototype.broker

import cs4k.prototype.broker.BrokerException.EnvironmentVariableException

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    /**
     * Name of environment variable for database URL.
     */
    private const val KEY_DB_URL = "DB_URL"

    /**
     * Get the database URL from the environment variable [KEY_DB_URL].
     * @return The database URL.
     * @throws EnvironmentVariableException If the environment variable for the database URL is missing.
     */
    fun getDbUrl() = System.getenv(KEY_DB_URL)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_DB_URL for the database URL.")
}
