package msg_broker_sketches.java

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Envelope
import redis.clients.jedis.JedisPooled
import redis.clients.jedis.JedisPubSub
import java.util.Collections

class TestConsumer(channel: Channel, val topics: List<String>): DefaultConsumer(channel) {

    override fun handleDelivery(p0: String?, p1: Envelope?, p2: AMQP.BasicProperties?, p3: ByteArray?) {
        requireNotNull(p1)
        requireNotNull(p3)
        val routingKey = p1.routingKey
        val deliveryTag = p1.deliveryTag
        val contentType = p2?.contentType
        val headers = p2?.headers
        val message = String(p3)
        val filter: String = headers?.get("x-stream-filter-value").toString()
        if(filter in topics)
            println("message received: topic: $filter, rk: $routingKey; ct: $contentType msg = $message")
        channel.basicAck(deliveryTag, false)
    }

}

class TestRedisHandler: JedisPubSub() {

    override fun onMessage(channel: String?, message: String?) {
        println("received message in channel $channel: $message")
    }


    override fun onSubscribe(channel: String?, subscribedChannels: Int) {
        println("subscribed to $channel")
        super.onSubscribe(channel, subscribedChannels)
    }

}

fun main() {
    rabbitMqSubscriber()
    // redisSubscriber()
}

fun rabbitMqSubscriber() {
    val factory = ConnectionFactory()
    factory.newConnection().use { connection ->
        val channel = connection.createChannel()
        channel.queueDeclare (
            STREAM_NAME,
            true,
            false, false,
            Collections.singletonMap("x-queue-type", "stream") as Map<String, String>
        )
        channel.basicQos(100)
        val topics = listOf("topic1")
        channel.basicConsume(
            STREAM_NAME,
            false,
            mapOf<String, Any>(
                "x-stream-filter" to topics.toTypedArray(),
                "x-stream-offset" to "first"
            ),
            TestConsumer(channel, topics)
        )
        while (channel.isOpen) {
            println("If done, press enter.")
            readln()
        }
    }
}

fun redisSubscriber() {
    val jedis = JedisPooled("localhost", 6379)
    val handler = TestRedisHandler()
    jedis.subscribe(handler, "notifications")
    while(!jedis.pool.isClosed) {
        println("If done, press enter.")
        readln()
    }
}
