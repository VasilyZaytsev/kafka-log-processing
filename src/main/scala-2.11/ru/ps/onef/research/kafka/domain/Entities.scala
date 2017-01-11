package ru.ps.onef.research.kafka.domain

import play.api.libs.json.{Json, OWrites, Reads}

/**
  * Created by Vasily.Zaytsev on 02.12.2016.
  */
object LogMessageLevel extends Enumeration {
  val TRACE = Value(5)
  val DEBUG = Value(4)
  val INFO = Value(3)
  val WARN = Value(2)
  val ERROR = Value(1)
  val UNKNOWN = Value(-1)

  //  implicit def msg2level(msg: LogMessage): LogMessageLevel.Value = LogMessageLevel.apply(msg.level)
  implicit def int2level(arg: Int): LogMessageLevel.Value = LogMessageLevel.apply(arg)
  implicit def level2Int(arg: LogMessageLevel.Value): Int = arg.id
}

case class LogMessage(level: Int, description: String, ts: Long, url: String = "www.example.com")


object LogMessage {
  implicit val writes: OWrites[LogMessage] = Json.writes[LogMessage]
  implicit val reads: Reads[LogMessage] = Json.reads[LogMessage]
//    {"level":5,"description":"Message 1","ts":1111111112,"url":"www.example.com"}
}