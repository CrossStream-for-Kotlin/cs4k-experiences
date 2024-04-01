package cs4k.prototype.http

object Uris {

    const val PREFIX = "/api"

    object Chat {
        const val LISTEN = "$PREFIX/chat/listen"
        const val SEND = "$PREFIX/chat/send"
    }

    object TicTacToe {
        const val START = "$PREFIX/game"
        const val WATCH = "$PREFIX/game/{id}"
        const val PLAY = "$PREFIX/game/{id}"
    }
}
