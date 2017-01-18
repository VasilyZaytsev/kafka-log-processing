package ru.ps.onef.research.storm

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.apache.storm.trident.operation.{BaseAggregator, TridentCollector}
import org.apache.storm.trident.tuple.TridentTuple
import org.slf4j.{Logger, LoggerFactory}
import ru.ps.onef.research.kafka.domain.LogMessageLevel.ERROR
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}
import ru.ps.onef.research.kafka.domain.{LogMessage, LogMessageLevel}

import scala.collection.mutable

/**
  * Created by Vasily.Zaytsev on 16.01.2017.
  */
class AlertingWindowAgg(val inputField: String) extends BaseAggregator[mutable.Map[String, (Long, Int)]] {

  @transient lazy private implicit val log: Logger = LoggerFactory.getLogger(classOf[AlertingWindowAgg])
  @transient lazy private val config = ConfigFactory load()
  @transient lazy private val stormConf = config getConfig "log.storm"
  @transient lazy private val alertConditionCount = stormConf getInt "alert.condition.count"
  @transient lazy private val alertTopicName = stormConf getString "alert.out.topic.name"

  override def init(batchId: scala.Any, collector: TridentCollector): mutable.Map[String, (Long, Int)] = mutable.Map.empty

  override def aggregate(partialValue: mutable.Map[String, (Long, Int)], tuple: TridentTuple, collector: TridentCollector): Unit = {
    ConsoleLogsConsumer.decodeTuple(tuple)(inputField, log)
      .filter(_.level == LogMessageLevel.ERROR.id)
      //It is possible that current window captured few source timestamps,
      //In current implementation last one will be used for result
      .foreach{ msg => partialValue.update(msg.url, (msg.ts, partialValue.getOrElse(msg.url, (0L,0))._2 + 1)) }
  }

  override def complete(completeValue: mutable.Map[String, (Long, Int)], collector: TridentCollector): Unit = {
    completeValue.foreach { case (url, (ts, counter)) if counter >= alertConditionCount =>
      LogsProducer.send(List(LogMessage(ERROR, s"$counter", ts, url)))(alertTopicName)
//        collector.emit()
    }
  }

}
