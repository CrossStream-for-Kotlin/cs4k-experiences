package psql_sketches

import org.postgresql.PGConnection
import java.sql.DriverManager
import java.sql.SQLException

fun main() {
    val url = System.getenv("DB_URL")
        ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
    val connection = DriverManager.getConnection(url)
    val pgConnection = connection.unwrap(PGConnection::class.java)
    val stm = connection.createStatement()
    stm.execute("LISTEN test")
    stm.close()
    try {
        while (true) {
            val notifications = pgConnection.notifications

            if (notifications != null) {
                for (i in notifications.indices)
                    println("NOTIFY: ${notifications[i].parameter}")
            }

            Thread.sleep(500)
        }
    } catch (ex: SQLException) {
        ex.printStackTrace()
    } catch (ie: InterruptedException) {
        ie.printStackTrace()
    }
}
