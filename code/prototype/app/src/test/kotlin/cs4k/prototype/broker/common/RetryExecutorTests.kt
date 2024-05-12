package cs4k.prototype.broker.common

import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.utils.SuccessTest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.sql.SQLException

class RetryExecutorTests {

    @Test
    fun `execute should return successfully on first try`() {
        // Arrange
        // Act
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = { SuccessTest }
        )

        // Assert
        assertEquals(SuccessTest, result)
    }

    @Test
    fun `execute should retry and succeed on second try`() {
        // Arrange
        var retries = 0

        // Act
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = {
                retries++
                if (retries == 1) throw SQLException("Fail on first attempt.")
                SuccessTest
            }
        )

        // Assert
        assertEquals(SuccessTest, result)
        assertEquals(2, retries)
    }

    @Test
    fun `execute should retry twice and succeed on third try`() {
        // Arrange
        var retries = 0

        // Act
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = {
                retries++
                if (retries == 1) throw SQLException("Fail on first attempt.")
                if (retries == 2) throw SQLException("Fail on second attempt.")
                SuccessTest
            }
        )

        // Assert
        assertEquals(SuccessTest, result)
        assertEquals(3, retries)
    }

    @Test
    fun `execute should throw after max retries`() {
        // Arrange
        var retries = 0

        // Assert [1]
        assertThrows(BrokerLostConnectionException::class.java) {
            // Act
            retryExecutor.execute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    throw SQLException("Always fails.")
                }
            )
        }

        // Assert [2]
        assertEquals(3, retries)
    }

    @Test
    fun `execute should not retry when retryCondition is false`() {
        // Arrange
        var retries = 0

        // Assert [1]
        assertDoesNotThrow {
            // Act
            retryExecutor.execute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    throw SQLException("Always fails.")
                },
                retryCondition = { it !is SQLException }
            )
        }

        // Assert [2]
        assertEquals(1, retries)
    }

    @Test
    fun `execute suspend should return successfully on first try`() {
        // Arrange
        runBlocking {
            // Act
            val result = retryExecutor.suspendExecute(
                exception = { BrokerLostConnectionException() },
                action = { SuccessTest }
            )

            // Assert
            assertEquals(SuccessTest, result)
        }
    }

    @Test
    fun `execute suspend should retry and succeed on second try`() {
        // Arrange
        var retries = 0
        runBlocking {
            // Act
            val result = retryExecutor.suspendExecute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    if (retries == 1) throw SQLException("Fail on first attempt.")
                    SuccessTest
                }
            )

            // Assert
            assertEquals(SuccessTest, result)
            assertEquals(2, retries)
        }
    }

    @Test
    fun `execute suspend should retry twice and succeed on third try`() {
        // Arrange
        var retries = 0
        runBlocking {
            // Act
            val result = retryExecutor.suspendExecute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    if (retries == 1) throw SQLException("Fail on first attempt.")
                    if (retries == 2) throw SQLException("Fail on second attempt.")
                    SuccessTest
                }
            )

            // Assert
            assertEquals(SuccessTest, result)
            assertEquals(3, retries)
        }
    }

    @Test
    fun `execute suspend should throw after max retries`() {
        // Arrange
        var retries = 0

        // Assert [1]
        assertThrows(BrokerLostConnectionException::class.java) {
            runBlocking {
                // Act
                retryExecutor.suspendExecute(
                    exception = { BrokerLostConnectionException() },
                    action = {
                        retries++
                        throw SQLException("Always fails.")
                    }
                )
            }
        }

        // Assert [2]
        assertEquals(3, retries)
    }

    @Test
    fun `execute suspend should not retry when retryCondition is false`() {
        // Arrange
        var retries = 0

        // Assert [1]
        assertDoesNotThrow {
            runBlocking {
                // Act
                retryExecutor.suspendExecute(
                    exception = { BrokerLostConnectionException() },
                    action = {
                        retries++
                        throw SQLException("Always fails.")
                    },
                    retryCondition = { it !is SQLException }
                )
            }
        }

        // Assert [2]
        assertEquals(1, retries)
    }

    private companion object {
        val retryExecutor = RetryExecutor(
            maxRetries = 3,
            waitTimeMillis = 1000
        )
    }
}
