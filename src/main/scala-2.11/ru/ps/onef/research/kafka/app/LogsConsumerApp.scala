package ru.ps.onef.research.kafka.app

import ru.ps.onef.research.kafka.ConsoleLogsConsumer

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsConsumerApp extends App {

  ConsoleLogsConsumer().read.foreach(println)
}
