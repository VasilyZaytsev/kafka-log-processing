package ru.ps.onef.research.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import play.api.libs.json.Json
import ru.ps.onef.research.kafka.domain.LogMessage

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
class ConsoleLogsConsumer {

  private lazy val producerConf = ConfigFactory load() getConfig "kafka.log.producer"

  private val props = new Properties()
  props put("group.id", "test")
  props put("enable.auto.commit", "true")
  props put("auto.commit.interval.ms", "1000")
  props put("session.timeout.ms", "30000")
  props put("zookeeper.connect", producerConf getString "zookeeper.connect")
  //  props put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  //  props put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  private val config = new ConsumerConfig(props)
  private val connector = Consumer.create(config)
  private val filterSpec = Whitelist(producerConf getString "topic")
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