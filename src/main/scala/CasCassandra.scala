import java.util.UUID
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.routing.{Broadcast, RoundRobinPool}
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.querybuilder.QueryBuilder
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object CasCassandra {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("CasCassandra")
    val session = Cluster.builder()
      .addContactPoint("cassandra-sg1-001")
      .addContactPoint("cassandra-sg1-002")
      .addContactPoint("cassandra-sg1-003")
      .build()
      .connect()

    val id = UUID.randomUUID()
    val writers = system.actorOf(RoundRobinPool(128).props(Props(classOf[CasWriter], id, session)), "writers")

    0 to 1000000 foreach { i =>
      writers ! i
    }

    writers ! Broadcast(PoisonPill)
    Await.ready(system.whenTerminated, Duration.Inf)

    val results = session.execute(
      QueryBuilder.select("counter").from("cas", "cas")
        .where(QueryBuilder.eq("id", id)))
    
    val counter = results.one().getLong("counter")    
    println(s"Counter value at $id: $counter")
  }
}
