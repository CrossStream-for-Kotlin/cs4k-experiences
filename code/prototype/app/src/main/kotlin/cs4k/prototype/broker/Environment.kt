package cs4k.prototype.broker

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
     * @throws Exception if the environment variable for the database URL is missing.
     */
    fun getDbUrl() = System.getenv(KEY_DB_URL) ?: throw Exception("Missing environment variable $KEY_DB_URL.")
}
