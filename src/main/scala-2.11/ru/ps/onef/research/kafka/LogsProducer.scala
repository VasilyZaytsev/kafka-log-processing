package ru.ps.onef.research.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import ru.ps.onef.research.utils

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsProducer {
  private val conf = ConfigFactory load()
  implicit val defaultTopic: String = conf getString "log.producer.topic"

  private val producer = new KafkaProducer[String, String](utils.propsFromConfig(conf getConfig "log.producer"))

  def send(message: String)(implicit topic: String): Unit = send(List(message))(topic)

  def send(messages: Seq[String])(implicit topic: String): Unit =
    try {
      println("sending batch messages  to kafka queue.......")
      messages.foreach { message =>
        producer.send(new ProducerRecord[String, String](topic, message))
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
}