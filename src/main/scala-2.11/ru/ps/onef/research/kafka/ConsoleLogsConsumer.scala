package ru.ps.onef.research.kafka

import java.util.Properties

import com.google.common.base.Throwables
import com.typesafe.config.ConfigFactory
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import org.apache.storm.tuple.ITuple
import org.slf4j.Logger
import play.api.libs.json.Json
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.utils

import scala.util.{Failure, Success, Try}

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

  def decodeTuple(input: ITuple)(implicit inputField: String, log: Logger): List[LogMessage] =
    Try(input.getBinaryByField(inputField)) match {
      case Success(bytes) if bytes != null =>
        ConsoleLogsConsumer.decode(bytes)
      case Success(_) =>
        log.error("Reading from input tuple returned null")
        List.empty
      case Failure(e) =>
        log.error("Could not read from input tuple: " + Throwables.getStackTraceAsString(e))
        List.empty
    }


  def apply(): ConsoleLogsConsumer = new ConsoleLogsConsumer(defaultTopicName, props)
  def apply(topicName: String): ConsoleLogsConsumer = new ConsoleLogsConsumer(topicName, props)
}