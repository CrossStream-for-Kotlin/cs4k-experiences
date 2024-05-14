package cs4k.prototype.broker.option3

import java.net.NetworkInterface

object Utils {

    fun getActiveMulticastNetworkInterface(): NetworkInterface {
        val networkInterfaces = NetworkInterface.getNetworkInterfaces()
        while (networkInterfaces.hasMoreElements()) {
            val networkInterface = networkInterfaces.nextElement()
            if (networkInterface.isUp && networkInterface.supportsMulticast()) {
                return networkInterface
            }
        }
        throw Exception("There is no active network interface that supports multicast!")
    }
}
