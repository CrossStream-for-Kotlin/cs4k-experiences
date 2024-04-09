package msg_broker_sketches.kotlin

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import io.github.crackthecodeabhi.kreds.connection.AbstractKredsSubscriber
import io.github.crackthecodeabhi.kreds.connection.Endpoint
import io.github.crackthecodeabhi.kreds.connection.newClient
import io.github.crackthecodeabhi.kreds.connection.newSubscriberClient
import io.github.crackthecodeabhi.kreds.connection.shutdown
import io.github.viartemev.rabbitmq.channel.confirmChannel
import io.github.viartemev.rabbitmq.channel.publish
import io.github.viartemev.rabbitmq.publisher.OutboundMessage
import io.github.viartemev.rabbitmq.queue.QueueSpecification
import io.github.viartemev.rabbitmq.queue.declareQueue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import msg_broker_sketches.java.STREAM_NAME
import java.util.*

fun main() {
    // rabbitMqPub()
    redisPub()
}

fun rabbitMqPub(): Unit = runBlocking {
    val connectionFactory = ConnectionFactory().apply { useNio() }
    connectionFactory.newConnection().use { connection ->
        connection.confirmChannel {
            declareQueue(
                QueueSpecification(
                    STREAM_NAME,
                )
            )
            while (true) {
                val msg = readln()
                if(msg == "") break
                publish {
                    publishWithConfirm(createMessage(msg))
                }
            }
        }
    }
}

private fun createMessage(body: String) =
    OutboundMessage(EXCHANGE_NAME, QUEUE_NAME, MessageProperties.PERSISTENT_BASIC, body)

class TestSubscriptionHandler: AbstractKredsSubscriber() {

    override fun onMessage(channel: String, message: String) {
        println("got new message from $channel: $message")
        super.onMessage(channel, message)
    }

    override fun onSubscribe(channel: String, subscribedChannels: Long) {
        println("subscribed to $channel")
        super.onSubscribe(channel, subscribedChannels)
    }

    override fun onException(ex: Throwable) {
        println("error: ${ex.stackTrace}")
    }

}

fun redisPub() = runBlocking {
    newClient(Endpoint.from("127.0.0.1:6379")).use { publisher ->
        println("Type a new message:")
        while (true) {
            val msg = readln()
            if(msg == "") break
            publisher.publish("notifications", msg)
            println("Message sent.")
        }
    }
}