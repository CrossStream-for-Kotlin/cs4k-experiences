package cs4k.prototype.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.domain.Game
import cs4k.prototype.domain.GameError
import org.postgresql.util.PGobject
import org.springframework.stereotype.Component
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

@Component
class TicTacToeRepository {

    init {
        createTables()
    }

    /**
     * Create the tables if they don't exist.
     */
    private fun createTables() {
        createConnection().use { conn ->
            val stm = conn.createStatement()
            stm.execute(
                "create table if not exists Games(id int generated always as identity primary key, board jsonb not null);" +
                    "create table if not exists Waiting(player varchar(16) primary key, gameId int unique)"
            )
        }
    }

    data class WaitingRegistry(
        val player: String,
        val gameId: Int
    )

    /**
     * Get the other player that is waiting for a game.
     * If there is no other player waiting, return null.
     * @param player the player that is waiting.
     */
    fun getOtherPlayer(player: String): WaitingRegistry? {
        createConnection().use { conn ->
            val stm = conn.prepareStatement("select player, gameId from Waiting limit 1")
            val rs = stm.executeQuery()
            if (!rs.next()) return null
            val other = rs.getString("player")
            val id = rs.getInt("gameId")
            if (other == player) throw GameError.AlreadyWaiting()
            return WaitingRegistry(player, id)
        }
    }

    /**
     * Register a player as waiting for a game.
     * @param game the game to be played.
     * @param player the player that is waiting.
     */
    fun registerWaiting(game: Game, player: String): Int {
        val gameId = createGame(game)
        standby(player, gameId)
        return gameId
    }

    /**
     * Create a new game in the database.
     * @param game the game to be created.
     */
    private fun createGame(game: Game): Int {
        createConnection().use { conn ->
            val stm = conn.prepareStatement(
                "insert into Games(board) values (?)",
                Statement.RETURN_GENERATED_KEYS
            )
            val json = PGobject()
            json.type = "jsonb"
            json.value = gameObjectToJson(game)
            stm.setObject(1, json)
            stm.executeUpdate()
            val rs = stm.generatedKeys
            if (!rs.next()) throw IllegalStateException("Should have generated id")
            return rs.getInt(1)
        }
    }

    /**
     * Register a player as waiting for a game.
     * @param player the player that is waiting.
     * @param gameId the id of the game.
     */
    private fun standby(player: String, gameId: Int) {
        createConnection().use { conn ->
            val stm = conn.prepareStatement(
                "insert into Waiting(player, gameId) values (?, ?)",
                Statement.RETURN_GENERATED_KEYS
            )
            stm.setString(1, player)
            stm.setInt(2, gameId)
            stm.executeUpdate()
        }
    }

    /**
     * Start a game with a player that is waiting.
     * @param game the game to be played.
     * @param gameId the id of the game.
     */
    fun startGame(game: Game, gameId: Int) {
        stopWaiting(gameId)
        updateGame(gameId, game)
    }

    /**
     * Stop a player from waiting for a game.
     * @param gameId the id of the game.
     */
    private fun stopWaiting(gameId: Int) {
        createConnection().use { conn ->
            val stm = conn.prepareStatement(
                "delete from Waiting where gameId = ?"
            )
            stm.setInt(1, gameId)
            stm.executeUpdate()
        }
    }

    /**
     * Get a game by its id.
     * @param id the id of the game.
     */
    fun getGame(id: Int): Game {
        createConnection().use { conn ->
            val stm = conn.prepareStatement("select board from Games where id = ? limit 1")
            stm.setInt(1, id)
            val rs = stm.executeQuery()
            if (!rs.next()) throw GameError.InvalidID()
            val gameJson = rs.getString("board")
            return gameJsonToObject(gameJson)
        }
    }

    /**
     * Update a game in the database.
     * @param id the id of the game.
     * @param game the game to be updated.
     */
    fun updateGame(id: Int, game: Game) {
        createConnection().use { conn ->
            val stm = conn.prepareStatement("update Games set board = ? where id = ?")
            val json = PGobject()
            json.type = "json"
            json.value = gameObjectToJson(game)
            stm.setObject(1, json)
            stm.setInt(2, id)
            stm.executeUpdate()
        }
    }

    /**
     * Create a JBDC connection.
     */
    private fun createConnection(): Connection {
        val url = System.getenv("DB_URL")
            ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
        return DriverManager.getConnection(url)
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        private fun gameObjectToJson(game: Game): String = objectMapper.writeValueAsString(game)
        private fun gameJsonToObject(json: String): Game = objectMapper.readValue(json, Game::class.java)
    }
}
