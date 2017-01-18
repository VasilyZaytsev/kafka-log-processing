package ru.ps.onef.research.storm

import java.time.LocalDateTime

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.storm.trident.operation.{BaseAggregator, TridentCollector}
import org.apache.storm.trident.tuple.TridentTuple
import org.slf4j.{Logger, LoggerFactory}
import ru.ps.onef.research.kafka.ConsoleLogsConsumer
import ru.ps.onef.research.kafka.domain.LogMessageLevel
import ru.ps.onef.research.storm.LogsSWindowAgg.AggregationValue

import scala.collection.mutable

/**
  * Created by Vasily.Zaytsev on 09.01.2017.
  */
class LogsSWindowAgg(val inputField: String, onCompleteCallBack: (Long, Map[String, LogsSWindowAgg.Statistic]) => Unit = { (ts,map) => }) extends BaseAggregator[AggregationValue] {

  @transient lazy private implicit val log: Logger = LoggerFactory.getLogger(classOf[LogsSWindowAgg])

  @transient lazy private val config = ConfigFactory load()
  @transient lazy private val dockerConf = config getConfig "docker.hbase"
  @transient lazy private val stormConf = config getConfig "log.storm"
  @transient lazy private val windowLength = stormConf getInt "window.length.secs"
  @transient lazy private val hbaseTableName = stormConf getString "table.name"
  @transient lazy private val hbaseConfig = {
    val conf = HBaseConfiguration.create()
    import HConstants._
    conf.set(ZOOKEEPER_QUORUM, dockerConf getString "image-host")
    conf
  }
  @transient lazy private val connection = ConnectionFactory.createConnection(hbaseConfig)

  import ru.ps.onef.research.kafka.domain.LogMessageLevel._

  override def init(batchId: scala.Any, collector: TridentCollector): AggregationValue = {
    AggregationValue(0L, mutable.Map.empty)
  }

  override def aggregate(partialValue: AggregationValue, tuple: TridentTuple, collector: TridentCollector): Unit = {

    ConsoleLogsConsumer.decodeTuple(tuple)(inputField, log) foreach { message =>
      val urlSt = partialValue.statistics.getOrElseUpdate(message.url, mutable.Map.empty)

      urlSt.update(message.level, urlSt.getOrElseUpdate(message.level, 0) + 1)

      if (partialValue.timestamp < message.ts) //looking for a max timestamp
        partialValue.timestamp = message.ts
    }
  }

  /**
    * Function save statistics to HBase
    * Since we didn't have guarantee on cleanup method
    * we should open and close connection on every write,
    *
    * @param outValue
    *                 timestamp of statistics
    *                 statistics url with counts per level
    */
  override def complete(outValue: AggregationValue, collector: TridentCollector): Unit = {
    val tableName = TableName.valueOf(hbaseTableName)
    val table = connection getTable tableName

//    Pasha metric
//    var totatlMsgs = 0
//    val start = LocalDateTime.now()
    try {
      val reverseTimestamps = Long.MaxValue - outValue.timestamp

      outValue.statistics.foreach { case (url, stat) =>
        val key = s"${url}_$reverseTimestamps"
        val thePut= new Put(Bytes.toBytes(key))

        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("url"),Bytes.toBytes(url))
        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("ts"),Bytes.toBytes(outValue.timestamp.toString))
        var total = 0
        stat.foreach { case (level, count) =>
          thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes(level.toString),Bytes.toBytes(count.toString))
          total += count
//          totatlMsgs += count
        }
        val rate = total.toDouble / windowLength
        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("rate"),Bytes.toBytes( rate.toString ) )
        table.put(thePut)
      }
    } finally {
      table.close()
    }

//    import java.time._
//    val end = LocalDateTime.now()
//    val duration = Duration.between(start, end).getNano
//    val approxRate = 1000000000d / duration
//    println(s"Window aggregating taked ${Math.round(duration / 1000000d)} count $totatlMsgs Approx throughput ${Math.round(approxRate * totatlMsgs)} ")
    onCompleteCallBack(outValue.timestamp, outValue.statistics.toMap)
  }

  /**
    * Important: no guarantee that this method will be executed and connection will be closed!
    */
  override def cleanup(): Unit = {
    connection.close()
    super.cleanup()
  }

  /**
    * Important: no guarantee that this method will be executed and connection will be closed!
    */
  override def finalize(): Unit = {
    connection.close()
    super.finalize()
  }
}

object LogsSWindowAgg {
  type Statistic = mutable.Map[LogMessageLevel.Value, Int]

  /**
    * Since this case class is a state for aggregator, timestamp declared as var
    * @param timestamp timestamp of statistics
    * @param statistics statistics url with counts per level
    */
  private[LogsSWindowAgg] case class AggregationValue(var timestamp: Long, statistics: mutable.Map[String, Statistic])
}
