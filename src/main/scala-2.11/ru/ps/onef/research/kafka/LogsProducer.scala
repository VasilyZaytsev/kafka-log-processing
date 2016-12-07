package ru.ps.onef.research.kafka

import java.util.concurrent.Future

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import play.api.libs.json.Json
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.utils

/**
  * Created by Vasily.Zaytsev on 01.12.2016.
  */
object LogsProducer {
  private val conf = ConfigFactory load()
  implicit val defaultTopic: String = conf getString "log.producer.topic"

  private val producer ={
    new KafkaProducer[String, String](utils.propsFromConfig(conf getConfig "log.producer.client.config"))
  }

  def send(message: List[LogMessage])(implicit topic: String): Seq[Future[RecordMetadata]] = {
    send(Seq(Json.stringify(Json.toJson(message))))(topic)
  }

  def send(message: String)(implicit topic: String): Seq[Future[RecordMetadata]] = send(Seq(message))(topic)

  def send(messages: Seq[String])(implicit topic: String): Seq[Future[RecordMetadata]] =
    try {
      messages.map { message => //sorry fellow map with side effect it is a bad idea but I think it is a easiest way
        producer.send(new ProducerRecord[String, String](topic, message))
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        Seq.empty
    }
}