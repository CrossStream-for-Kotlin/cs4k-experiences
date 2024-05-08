package cs4k.prototype.broker.common

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import kotlin.test.Test

class EnvironmentTests {

    @Test
    fun `get postgreSQL database url`() {
        assertDoesNotThrow {
            Environment.getPostgreSQLDbUrl()
        }
    }

    @Test
    fun `get redis host`() {
        assertDoesNotThrow {
            Environment.getRedisHost()
        }
    }

    @Test
    fun `get redis port`() {
        assertDoesNotThrow {
            Environment.getRedisPort()
        }
    }
}
