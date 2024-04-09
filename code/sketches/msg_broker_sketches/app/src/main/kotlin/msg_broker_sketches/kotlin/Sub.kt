package msg_broker_sketches.kotlin

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Delivery
import io.github.crackthecodeabhi.kreds.connection.Endpoint
import io.github.crackthecodeabhi.kreds.connection.newSubscriberClient
import io.github.crackthecodeabhi.kreds.connection.shutdown
import io.github.viartemev.rabbitmq.channel.channel
import io.github.viartemev.rabbitmq.channel.consume
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

fun ioIntensiveFunction(it: Delivery) {
    println(message = "Message: ${String(it.body)}")
}

val handler: suspend (Delivery) -> Unit =
    { delivery: Delivery -> withContext(Dispatchers.IO) { ioIntensiveFunction(delivery) } }

fun main() {
    //rabbitMqSub()
    redisSub()
}

fun rabbitMqSub() = runBlocking {
    val connectionFactory = ConnectionFactory().apply { useNio() }
    connectionFactory.newConnection().use { connection ->
        connection.channel {
            try {
                consume(QUEUE_NAME, 1) {
                    (1..TIMES).map { async(Dispatchers.IO) { consumeMessageWithConfirm(handler) } }.awaitAll()
                }
            } catch (e: RuntimeException) {
                println("Error is here...let's rollback handler actions")
            }
        }
    }
}

fun redisSub() = runBlocking(Dispatchers.IO) {
    coroutineScope {
        newSubscriberClient(Endpoint.from("127.0.0.1:6379"), TestSubscriptionHandler()).use { client ->
            println("Connected! Awaiting responses...")
            client.subscribe("notifications")
            readln()
            client.unsubscribe("notifications")
        }
    }
    shutdown()
}