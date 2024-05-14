/**
 * Class that represents a neighbor
 */
class Neighbor(val ip: String, val port: Int) {
    //lateinit var socket: Socket

    /**
     * Connect to the neighbor and informs it about the new neighbor
     */
    fun connect(myPort: Int, myIp: String) {
        try {
            val socket = Socket(ip.trim(), port)
            val out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            out.write("new_neighbor;$myIp:$myPort")
            out.newLine()
            out.flush()
            out.close()
        } catch (e: Exception) {
            println("Erro ao conectar-se ao vizinho $ip:$port -> ${e.message}")
            throw e
        }
    }

    /**
     * Send a message to the neighbor
     */

    fun sendMessage(message: String) {
        try {
            val socket = Socket(ip.trim(), port)
            val out = BufferedWriter(OutputStreamWriter(socket.getOutputStream()))
            out.write(message)
            out.newLine()
            out.flush()
            out.close()
        }catch (e: Exception) {
            println("Erro ao enviar mensagem para o vizinho $ip:$port -> ${e.message}")
            throw e
        }
    }
}
