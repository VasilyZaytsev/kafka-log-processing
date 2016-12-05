name := "KafkaLogsConsumer"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.0",
  "com.typesafe.play" % "play-json_2.11" % "2.5.10",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"
)