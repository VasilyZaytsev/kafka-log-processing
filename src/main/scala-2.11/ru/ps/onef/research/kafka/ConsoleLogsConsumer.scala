package ru.ps.onef.research.kafka

import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import play.api.libs.json.Json
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.utils

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
class ConsoleLogsConsumer {

  private val conf = ConfigFactory load()
  private val props = utils.propsFromConfig(conf getConfig "log.consumer")

  private val connector = Consumer.create(new ConsumerConfig(props))
  private val filterSpec = Whitelist(conf getString "log.producer.topic")
  private val logsStream = connector.createMessageStreamsByFilter(filterSpec).head

  def read: Stream[List[LogMessage]] = Stream.cons({
    val msg:Array[Byte] = logsStream.head.message
    Json.parse(msg).asOpt[List[LogMessage]].getOrElse(List.empty)
  }, read)

  def shutdown = connector.shutdown()
}

object ConsoleLogsConsumer {
  def apply(): ConsoleLogsConsumer = new ConsoleLogsConsumer()
}