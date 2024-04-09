package psql_sketches

import java.sql.DriverManager

fun main() {
    val url = System.getenv("DB_URL")
        ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
    while (true) {
        val line = readlnOrNull() ?: return
        val connection = DriverManager.getConnection(url)
        val stm = connection.createStatement()
        stm.execute("NOTIFY test, '$line'")
        stm.close()
    }
}