name := "KafkaLogsConsumer"

version := "1.0"

scalaVersion := "2.11.8"
val stormVersion = "1.0.2"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"
//    exclude("javax.jms", "jms")
//    exclude("com.sun.jdmk", "jmxtools")
//    exclude("com.sun.jmx", "jmxri")
    exclude("org.slf4j", "slf4j-simple")
    exclude("log4j", "log4j"),
//    exclude("org.apache.zookeeper", "zookeeper")
//    exclude("com.101tec", "zkclient"),
  "com.typesafe.play" % "play-json_2.11" % "2.5.10",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "org.apache.storm" % "storm-core" % stormVersion % "provided"
//    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.slf4j", "log4j-over-slf4j"),
  "org.apache.storm" % "storm-kafka" % stormVersion,
//    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.curator" % "curator-test" % "3.2.1"
    exclude("org.jboss.netty", "netty")
    exclude("org.slf4j", "slf4j-log4j12"),
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)