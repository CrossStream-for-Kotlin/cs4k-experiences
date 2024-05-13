package cs4k.prototype.broker.option2.redis

import cs4k.prototype.broker.common.BrokerException
import org.junit.jupiter.api.assertDoesNotThrow
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BrokerRedisTests {

    @Test
    fun `can create a BrokerRedis with a acceptable database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            BrokerRedis(dbConnectionPoolSize = ACCEPTABLE_CONNECTION_POOL_SIZE)
        }
    }

    @Test
    fun `cannot create a BrokerRedis with a big database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerRedis(dbConnectionPoolSize = BIG_CONNECTION_POOL_SIZE)
        }
    }

    @Test
    fun `cannot create a BrokerRedis with a negative database connection pool size`() {
        // Arrange
        // Act
        // Assert
        assertFailsWith<BrokerException.ConnectionPoolSizeException> {
            BrokerRedis(dbConnectionPoolSize = NEGATIVE_CONNECTION_POOL_SIZE)
        }
    }

    companion object {
        private const val ACCEPTABLE_CONNECTION_POOL_SIZE = 10
        private const val BIG_CONNECTION_POOL_SIZE = 10_000
        private const val NEGATIVE_CONNECTION_POOL_SIZE = -10_000
    }
}
