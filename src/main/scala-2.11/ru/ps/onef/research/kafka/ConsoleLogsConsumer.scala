package ru.ps.onef.research.kafka

import java.util.Properties

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
  //  props put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  //  props put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  private val config = new ConsumerConfig(props)
  private val connector = Consumer.create(config)
  private val filterSpec = Whitelist(conf getString "log.producer.topic")
  private val logsStream = connector.createMessageStreamsByFilter(filterSpec).head

  def read: Stream[List[LogMessage]] = Stream.cons({
    val msg:Array[Byte] = logsStream.head.message
//    new String(msg)
    Json.parse(msg).asOpt[List[LogMessage]].getOrElse(List.empty)
  }, read)
}

object ConsoleLogsConsumer {
  def apply(): ConsoleLogsConsumer = new ConsoleLogsConsumer()
}