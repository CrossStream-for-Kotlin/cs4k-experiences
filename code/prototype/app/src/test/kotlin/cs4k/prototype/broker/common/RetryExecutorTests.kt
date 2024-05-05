package cs4k.prototype.broker.common

import cs4k.prototype.broker.common.BrokerException.BrokerLostConnectionException
import cs4k.prototype.utils.SuccessTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.sql.SQLException

class RetryExecutorTests {

    @Test
    fun `execute should return successfully on first try`() {
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = { SuccessTest }
        )
        assertEquals(SuccessTest, result)
    }

    @Test
    fun `execute should retry and succeed on second try`() {
        var retries = 0
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = {
                retries++
                if (retries == 1) throw SQLException("Fail on first attempt.")
                SuccessTest
            }
        )
        assertEquals(SuccessTest, result)
        assertEquals(2, retries)
    }

    @Test
    fun `execute should retry twice and succeed on third try`() {
        var retries = 0
        val result = retryExecutor.execute(
            exception = { BrokerLostConnectionException() },
            action = {
                retries++
                if (retries == 1) throw SQLException("Fail on first attempt.")
                if (retries == 2) throw SQLException("Fail on second attempt.")
                SuccessTest
            }
        )
        assertEquals(SuccessTest, result)
        assertEquals(3, retries)
    }

    @Test
    fun `execute should throw after max retries`() {
        var retries = 0
        assertThrows(BrokerLostConnectionException::class.java) {
            retryExecutor.execute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    throw SQLException("Always fails.")
                }
            )
        }
        assertEquals(3, retries)
    }

    @Test
    fun `execute should not retry when retryCondition is false`() {
        var retries = 0
        assertThrows(SQLException::class.java) {
            retryExecutor.execute(
                exception = { BrokerLostConnectionException() },
                action = {
                    retries++
                    throw SQLException("Always fails.")
                },
                retryCondition = { it !is SQLException }
            )
        }
        assertEquals(1, retries)
    }

    private companion object {
        val retryExecutor = RetryExecutor(
            maxRetries = 3,
            waitTimeMillis = 1000
        )
    }
}
