name := "cas-cassandra"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.12",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
)
