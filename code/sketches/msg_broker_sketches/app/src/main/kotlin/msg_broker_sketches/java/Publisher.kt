package msg_broker_sketches.java

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import redis.clients.jedis.JedisPooled
import java.util.*

fun main() {
    rabbitMqPublisher()
    // redisPublisher()
}

fun rabbitMqPublisher() {
    val factory = ConnectionFactory()
    while (true) {
        val msg = readln()
        if(msg == "") return
        factory.newConnection().use { connection ->
            val channel = connection.createChannel()
            channel.queueDeclare (
                STREAM_NAME,
                true,
                false, false,
                Collections.singletonMap("x-queue-type", "stream") as Map<String, String>
            )
            channel.basicPublish(
                "",
                STREAM_NAME,
                AMQP.BasicProperties.Builder()
                    .headers(
                        Collections.singletonMap(
                            "x-stream-filter-value", "topic2"
                        ) as Map<String, String>
                    ).build(),
                msg.toByteArray()
            )
        }
    }
}

fun redisPublisher() {
    val jedis = JedisPooled("localhost", 6379)
    while(!jedis.pool.isClosed) {
        val msg = readln()
        if (msg == "") return
        jedis.publish("notifications", msg)
    }

}