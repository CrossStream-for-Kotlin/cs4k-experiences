package cs4k.prototype.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import cs4k.prototype.broker.Notifier
import cs4k.prototype.domain.Game
import cs4k.prototype.domain.GameError
import cs4k.prototype.domain.GameInfo
import org.postgresql.util.PGobject
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.concurrent.TimeUnit

@Component
class TicTacToeRepository(
    val notifier: Notifier
) {

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

    class WaitingRegistry(
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
    fun registerWaiting(game: Game, player: String): SseEmitter {
        val gameId = createGame(game)
        standby(player, gameId)
        return listenAndInitialNotify(gameId, game)
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
    fun startGame(game: Game, gameId: Int): SseEmitter {
        stopWaiting(gameId)
        updateGame(gameId, game)
        return listenAndInitialNotify(gameId, game)
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
        notifyGameState(id, game)
    }

    /**
     * Create a JBDC connection.
     */
    private fun createConnection(): Connection {
        val url = System.getenv("DB_URL")
            ?: throw IllegalAccessException("No connection URL given - define DB_URL environment variable")
        return DriverManager.getConnection(url)
    }

    /**
     * Listen for changes in the game state and notify the player.
     * @param gameId the id of the game.
     * @param game the game to be played.
     */
    private fun listenAndInitialNotify(gameId: Int, game: Game): SseEmitter {
        val sseEmitter = SseEmitter(TimeUnit.MINUTES.toMillis(5))

        notifier.subscribe(
            topic = gameId.toString(),
            handler = { event ->
                val sseEmitterEvent = SseEmitter.event()
                    .name(event.topic)
                    .id(event.id.toString())
                    .data("event: ${event.topic} - id: ${event.id} - data: ${event.message}")
                sseEmitter.send(sseEmitterEvent)

                if (event.isLast) sseEmitter.complete()
            }
        )
        notifyGameState(gameId, game)

        return sseEmitter
    }

    /**
     * Notify the player of the game state.
     * @param gameId the id of the game.
     * @param game the game to be played.
     */
    private fun notifyGameState(gameId: Int, game: Game) {
        val gameInfo = GameInfo(gameId, game)
        notifier.publish(
            topic = "gameId$gameId",
            message = gameInfoObjectToJson(gameInfo)
        )
    }

    companion object {
        private val objectMapper = ObjectMapper().registerModules(KotlinModule.Builder().build())
        private fun gameObjectToJson(game: Game): String = objectMapper.writeValueAsString(game)
        private fun gameJsonToObject(json: String): Game = objectMapper.readValue(json, Game::class.java)

        private fun gameInfoObjectToJson(gameInfo: GameInfo) = objectMapper.writeValueAsString(gameInfo)
    }
}
