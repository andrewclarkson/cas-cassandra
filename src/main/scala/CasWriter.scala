import java.util.UUID

import akka.actor.Actor
import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.datastax.driver.core.exceptions.{NoHostAvailableException, ReadTimeoutException, WriteTimeoutException}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Update}


class CasWriter(id: UUID, session: Session) extends Actor {

  private[this] def incrementCounter: Unit = {
    var retries = 0
    while(true) {
      try {
        val counter = session.execute(
          QueryBuilder.select("counter")
            .from("cas", "cas")
            .where(QueryBuilder.eq("id", id))
            .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
          .one().getLong("counter")
        val results = session.execute(
          QueryBuilder.update("cas", "cas")
            .where(QueryBuilder.eq("id", id))
            .`with`(QueryBuilder.set("counter", counter + 1))
            .onlyIf(QueryBuilder.eq("counter", counter))
            .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
            .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
        if(!results.wasApplied()) {
          println(s"CAS contention $retries")
        } else {
          return
        }
      } catch {
        case e: NoHostAvailableException =>
          println("NoHostAvailableException")
        case e: WriteTimeoutException =>
          println(s"WriteTimeout: ${e.getWriteType}")
        case e: ReadTimeoutException =>
          println(s"ReadTimeout")
      }
      retries += 1
    }
  }

  def receive = {
    case i: Int => incrementCounter
  }
}
