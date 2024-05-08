package cs4k.prototype.broker.common

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import kotlin.test.Test

class EnvironmentTests {

    @Test
    fun `get postgreSQL database url`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            Environment.getPostgreSQLDbUrl()
        }
    }

    @Test
    fun `get redis host`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            Environment.getRedisHost()
        }
    }

    @Test
    fun `get redis port`() {
        // Arrange
        // Act
        // Assert
        assertDoesNotThrow {
            Environment.getRedisPort()
        }
    }
}
