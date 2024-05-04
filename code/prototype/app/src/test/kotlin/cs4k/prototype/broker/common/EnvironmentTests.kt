package cs4k.prototype.broker.common

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import kotlin.test.Test

class EnvironmentTests {

    @Test
    fun `get database url`() {
        assertDoesNotThrow {
            Environment.getPostgreSQLDbUrl()
        }
    }
}
