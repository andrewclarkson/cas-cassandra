import java.util.UUID

import akka.actor.Actor
import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.datastax.driver.core.exceptions.{ReadTimeoutException, WriteTimeoutException}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}

import scala.annotation.tailrec


class CasWriter(id: UUID, session: Session) extends Actor {

  @tailrec
  private[this] def incrementCounter(retries: Int): Unit = {
    val results = try {
      val counter = session.execute(
        QueryBuilder.select("counter")
          .from("cas", "cas")
          .where(QueryBuilder.eq("id", id))
          .setConsistencyLevel(ConsistencyLevel.SERIAL)
      ).one().getLong("counter")
      session.execute(
        QueryBuilder.update("cas", "cas")
          .where(QueryBuilder.eq("id", id))
          .`with`(QueryBuilder.set("counter", counter + 1))
          .onlyIf(QueryBuilder.eq("counter", counter)))
    } catch {
      case e: WriteTimeoutException =>
        println(s"WriteTimeout: ${e.getWriteType}")
        return incrementCounter(retries + 1)
      case e: ReadTimeoutException =>
        println(s"ReadTimeout")
        return incrementCounter(retries + 1)
    }
    if(!results.wasApplied()) {
      println(s"CAS contention $retries")
      return incrementCounter(retries + 1)
    }
  }

  def receive = {
    case i: Int => incrementCounter(0)
  }
}
