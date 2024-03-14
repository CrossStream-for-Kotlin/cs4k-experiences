package cs4k.prototype.http

object Uris {

    const val PREFIX = "/api"

    object Chat {
        const val LISTEN = "$PREFIX/chat/listen"
        const val SEND = "$PREFIX/chat/send"
        const val LISTEN_GROUP = "$PREFIX/chat/listen/{group}"
        const val SEND_TO_GROUP = "$PREFIX/chat/send/{group}"
    }

    object TicTacToe {
        const val START = "$PREFIX/game"
        const val PLAY = "$PREFIX/game/{id}"
    }
}
