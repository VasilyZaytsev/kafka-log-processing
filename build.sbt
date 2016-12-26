name := "KafkaLogsConsumer"

version := "1.0"

scalaVersion := "2.11.8"
val stormVersion = "1.0.2"
val dockerTestkitVersion = "0.9.0-RC1"
val hbaseVersion = "1.2.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
    exclude("org.slf4j", "slf4j-simple")
    exclude("log4j", "log4j"),
  "com.typesafe.play" % "play-json_2.11" % "2.5.10",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "org.apache.storm" % "storm-core" % stormVersion % "provided"
    exclude("org.slf4j", "log4j-over-slf4j"),
  "org.apache.storm" % "storm-kafka" % stormVersion,
  "org.apache.curator" % "curator-test" % "3.2.1"
    exclude("org.jboss.netty", "netty")
    exclude("org.slf4j", "slf4j-log4j12"),
  "com.whisk" %% "docker-testkit-scalatest" % dockerTestkitVersion % "test",
  "com.whisk" %% "docker-testkit-config" % dockerTestkitVersion % "test"
    exclude("com.google.guava","guava"),
  "com.whisk" %% "docker-testkit-impl-spotify" % dockerTestkitVersion % "test"
    exclude("com.google.guava","guava"),
  "org.apache.hadoop" % "hadoop-common" % "2.6.1"
    exclude("com.google.guava","guava")
    exclude("com.sun.jersey","jersey-core")
    exclude("com.sun.jersey","jersey-json")
    exclude("com.sun.jersey","jersey-server"),
  "org.apache.hbase" % "hbase" % hbaseVersion,
  "org.apache.hbase" % "hbase-client" % hbaseVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion
)