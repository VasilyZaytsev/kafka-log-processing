package ru.ps.onef.research.kafka.app

import ru.ps.onef.research.embeded.{EKafka, EZooKeeper}
import ru.ps.onef.research.kafka.ConsoleLogsConsumer

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsConsumerApp extends App {
  EZooKeeper()
  EKafka().start()

  ConsoleLogsConsumer().read.foreach(println)

  EKafka().stop()
  EZooKeeper().stop
}
