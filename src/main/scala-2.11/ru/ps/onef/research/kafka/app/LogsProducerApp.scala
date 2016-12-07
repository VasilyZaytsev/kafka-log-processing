package ru.ps.onef.research.kafka.app

import play.api.libs.json.Json
import ru.ps.onef.research.kafka.LogsProducer
import ru.ps.onef.research.kafka.LogsProducer._
import ru.ps.onef.research.kafka.domain.LogMessageLevel._
import ru.ps.onef.research.kafka.domain.{LogMessage}

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsProducerApp extends App {

  val batchSize = 10

  (1 to 100).toList.map{ n => LogMessage(TRACE, "Message " + n, 0L) }.grouped(batchSize).foreach { listMsgs =>
    println("Sending message batch size " + listMsgs.length)
    LogsProducer send { Json.stringify(Json.toJson(listMsgs)) }
  }
}
