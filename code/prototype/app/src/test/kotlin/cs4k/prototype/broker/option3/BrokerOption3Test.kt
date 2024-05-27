package cs4k.prototype.broker.option3

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mockito
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.test.Test
import kotlin.test.assertEquals

class BrokerOption3Test {

    private lateinit var broker1: BrokerOption3
    private lateinit var broker2: BrokerOption3
    private lateinit var broker3: BrokerOption3

    @BeforeEach
    fun setUp() {
        val networkInterface = Mockito.mock(NetworkInterface::class.java)
        Mockito.`when`(networkInterface.isUp).thenReturn(true)
        Mockito.`when`(networkInterface.supportsMulticast()).thenReturn(true)

        val inetAddresses = Collections.enumeration(
            listOf(
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("127.0.0.2"),
                InetAddress.getByName("127.0.0.3")

            )
        )
        Mockito.`when`(networkInterface.inetAddresses).thenReturn(inetAddresses)

        broker1 = BrokerOption3("127.0.0.1", networkInterface = networkInterface)
        broker2 = BrokerOption3("127.0.0.2", networkInterface = networkInterface)
        broker3 = BrokerOption3("127.0.0.3", networkInterface = networkInterface)

        // wait for the multicast service to start
        Thread.sleep(50000)
        println("Multicast service started")
    }

    @AfterEach
    fun tearDown() {
        broker1.shutdown()
        broker2.shutdown()
        broker3.shutdown()
    }

    @Test
    fun `test communication between two brokers`() {
        val topic = "test-topic"
        val message = "Hello from broker1"
        val latch = CountDownLatch(1)

        broker1.subscribe(topic, handler = { event ->
            assertEquals(event.message, message)
            latch.countDown()
        })

        broker2.publish(topic, message, false)

        broker1.toTest()
        println("Broker1 toTest() called")
        broker2.toTest()
        broker3.toTest()

        latch.await()
    }
}
