package ru.ps.onef.research.storm

import com.google.common.base.Throwables
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.{Logger, LoggerFactory}
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}

import scala.util.{Failure, Success, Try}

/**
  * Created by Vasily.Zaytsev on 08.12.2016.
  */
class SimpleUpdateBolt(outputTopicName: String, implicit val inputField: String = "bytes") extends BaseBasicBolt {

  // Must be transient because Logger is not serializable
  @transient lazy private implicit val log: Logger = LoggerFactory.getLogger(classOf[SimpleUpdateBolt])

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
    val updatedList = ConsoleLogsConsumer decodeTuple input map { log =>
      log.copy(description = s"${log.description} processed with ${this.getClass.getName}")
    }
    LogsProducer.sendSeq(updatedList)(outputTopicName)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
  }
}
