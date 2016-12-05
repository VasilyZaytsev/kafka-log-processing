package ru.ps.onef.research.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsProducer {
  private val producerConf = ConfigFactory load() getConfig "kafka.log.producer"
  implicit val defaultTopic: String = producerConf getString "topic"

  private val props = new Properties()
  props put("bootstrap.servers", producerConf getString "bootstrap.servers")
  props put("acks", "all")
  props put("retries", 3.toString)
  props put("batch.size", 16384.toString)
  props put("linger.ms", 1.toString)
  props put("buffer.memory", 33554432.toString)
  props put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)

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