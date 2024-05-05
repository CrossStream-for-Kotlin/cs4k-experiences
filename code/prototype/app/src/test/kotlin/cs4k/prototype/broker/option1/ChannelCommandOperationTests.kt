package cs4k.prototype.broker.option1

import kotlin.test.Test
import kotlin.test.assertEquals

class ChannelCommandOperationTests {

    @Test
    fun `check 'Listen' toString override`() {
        assertEquals("listen", "${ChannelCommandOperation.Listen}")
    }

    @Test
    fun `check 'UnListen' toString override`() {
        assertEquals("unlisten", "${ChannelCommandOperation.UnListen}")
    }
}
