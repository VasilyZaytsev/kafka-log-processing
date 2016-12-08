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
class ConsoleLogsConsumer(topicName: String, props: Properties) {
  private val connector = Consumer.create(new ConsumerConfig(props))
  private val filterSpec = Whitelist(topicName)
  private val logsStream = connector.createMessageStreamsByFilter(filterSpec).head

  def read: Stream[List[LogMessage]] = Stream.cons({
    ConsoleLogsConsumer.decode(logsStream.head.message)
  }, read)

  def shutdown(): Unit = connector.shutdown()
}

object ConsoleLogsConsumer {
  private val conf = ConfigFactory load()
  private val props = utils.propsFromConfig(conf getConfig "log.consumer.config")

  val defaultTopicName: String = conf getString "log.consumer.topic"

  def decode(msg: Array[Byte]):List[LogMessage] = Json.parse(msg).asOpt[List[LogMessage]].getOrElse(List.empty)

  def apply(): ConsoleLogsConsumer = new ConsoleLogsConsumer(defaultTopicName, props)
  def apply(topicName: String): ConsoleLogsConsumer = new ConsoleLogsConsumer(topicName, props)
}