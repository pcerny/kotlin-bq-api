import java.time.Instant
import java.time.format.DateTimeFormatter

object SimpleLog {
    fun log(msg: String) =
        println("${DateTimeFormatter.ISO_INSTANT.format(Instant.now())} [${Thread.currentThread().name}] $msg")
}
