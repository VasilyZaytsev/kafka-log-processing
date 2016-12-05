package ru.ps.onef.research.kafka.domain

import play.api.libs.json.Json

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

  implicit def level2Int(arg: LogMessageLevel.Value): Int = arg.id
  implicit def msg2level(msg: LogMessage): LogMessageLevel.Value = LogMessageLevel.apply(msg.level)
//  implicit def int2level(arg: Int): LogMessageLevel.Value = LogMessageLevel.apply(arg)
}

case class LogMessage(level: Int, description: String, ts: Long)
object LogMessage {
  implicit val writes = Json.writes[LogMessage]
  implicit val reads = Json.reads[LogMessage]
}