package cs4k.prototype.broker.common

import cs4k.prototype.broker.common.BrokerException.EnvironmentVariableException

/**
 * Responsible for accessing environment variables.
 */
object Environment {

    // Name of environment variable for PostgreSQL database URL.
    private const val KEY_POSTGRESQL_DB_URL = "POSTGRESQL_DB_URL"

    // Name of environment variable for Redis host.
    private const val KEY_REDIS_HOST = "REDIS_HOST"

    // Name of environment variable for Redis port.
    private const val KEY_REDIS_PORT = "REDIS_PORT"

    // Name of environment variable for hostname.
    private const val KEY_HOSTNAME = "HOSTNAME"

    // Name of environment variable for service name.
    private const val KEY_SERVICE_NAME = "SERVICE_NAME"

    /**
     * Get the PostgreSQL database URL from the environment variable [KEY_POSTGRESQL_DB_URL].
     *
     * @return The PostgreSQL database URL.
     * @throws EnvironmentVariableException If the environment variable for the PostgreSQL database URL is missing.
     */
    fun getPostgreSQLDbUrl() = System.getenv(KEY_POSTGRESQL_DB_URL)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_POSTGRESQL_DB_URL.")

    /**
     * Get the Redis host from the environment variable [KEY_REDIS_HOST].
     *
     * @return Redis host name.
     * @throws EnvironmentVariableException If the environment variable for the Redis host is missing.
     */
    fun getRedisHost() = System.getenv(KEY_REDIS_HOST)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_REDIS_HOST.")

    /**
     * Get the Redis port from the environment variable [KEY_REDIS_PORT].
     *
     * @return Redis port number.
     * @throws EnvironmentVariableException If the environment variable for the Redis port is missing.
     */
    fun getRedisPort() = System.getenv(KEY_REDIS_PORT)?.toInt()
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_REDIS_PORT.")

    /**
     * Get the hostname from the environment variable [KEY_HOSTNAME].
     *
     * @return The hostname.
     * @throws EnvironmentVariableException If the environment variable for the hostname is missing.
     */
    fun getHostname() = System.getenv(KEY_HOSTNAME)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_HOSTNAME.")

    /**
     * Get the service name from the environment variable [KEY_SERVICE_NAME].
     *
     * @return The service name.
     * @throws EnvironmentVariableException If the environment variable for the service name is missing.
     */
    fun getServiceName() = System.getenv(KEY_SERVICE_NAME)
        ?: throw EnvironmentVariableException("Missing environment variable $KEY_SERVICE_NAME.")
}
