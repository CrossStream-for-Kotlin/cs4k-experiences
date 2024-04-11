package cs4k.prototype.broker

import cs4k.prototype.broker.BrokerException.EnvironmentVariableException

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    // Name of environment variable for database URL.
    private const val KEY_DB_URL = "DB_URL"

    // Name of environment variable for redis host.
    private const val KEY_REDIS_HOST = "REDIS_HOST"

    // Name of environment variable for redis port.
    private const val KEY_REDIS_PORT = "REDIS_PORT"

    /**
     * Get the database URL from the environment variable [KEY_DB_URL].
     *
     * @return The database URL.
     * @throws EnvironmentVariableException If the environment variable for the database URL is missing.
     */
    fun getDbUrl() = System.getenv(KEY_DB_URL)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_DB_URL for the database URL.")

    /**
     * Get the database URL from the environment variable [KEY_REDIS_HOST].
     *
     * @return Redis host name.
     * @throws EnvironmentVariableException If the environment variable for the database URL is missing.
     */
    fun getRedisHost() = System.getenv(KEY_REDIS_HOST)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_REDIS_HOST.")

    /**
     * Get the database URL from the environment variable [KEY_REDIS_PORT].
     *
     * @return Redis port number
     * @throws EnvironmentVariableException If the environment variable for the database URL is missing.
     */
    fun getRedisPort() = System.getenv(KEY_REDIS_PORT)?.toInt()
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_REDIS_PORT.")
}
