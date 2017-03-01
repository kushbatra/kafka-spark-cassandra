name := "KafkaSparkCassandraDemo"

version := "1.0"

organization := "kush.demo"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.0.0",
"com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-RC1",
"com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
("org.apache.spark" %% "spark-streaming-kafka" % "1.6.0").
exclude("org.spark-project.spark", "unused")
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}