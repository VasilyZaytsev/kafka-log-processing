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
class SimpleUpdateBolt(outputTopicName: String, inputField: String = "bytes") extends BaseBasicBolt {

  // Must be transient because Logger is not serializable
  @transient lazy private val log: Logger = LoggerFactory.getLogger(classOf[SimpleUpdateBolt])

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
    val readTry = Try(input.getBinaryByField(inputField))
    readTry match {
      case Success(bytes) if bytes != null =>
        val updatedList = ConsoleLogsConsumer.decode(bytes)
          .map(log => log.copy(description = s"${log.description} processed with ${this.getClass.getName}"))

        LogsProducer.send(updatedList)(outputTopicName)
      case Success(_) => log.error("Reading from input tuple returned null")
      case Failure(e) => log.error("Could not read from input tuple: " + Throwables.getStackTraceAsString(e))
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
  }
}
