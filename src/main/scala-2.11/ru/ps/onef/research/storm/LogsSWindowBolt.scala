package ru.ps.onef.research.storm

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.windowing.TupleWindow
import org.slf4j.{Logger, LoggerFactory}
import ru.ps.onef.research.kafka.ConsoleLogsConsumer
import ru.ps.onef.research.kafka.domain.LogMessageLevel

import scala.collection.mutable

/**
  * Created by Vasily.Zaytsev on 29.12.2016.
  *
  * NOTE: This implementation is currently not in use!
  */
class LogsSWindowBolt(@transient conf: Configuration, implicit val inputField: String = "bytes") extends BaseWindowedBolt {

  @transient lazy private implicit val log: Logger = LoggerFactory.getLogger(classOf[LogsSWindowBolt])

  private var collector: OutputCollector = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
  }

  private val config = ConfigFactory load()
  private val logConf = config getConfig "log.storm.bolt"
  private val windowLength = logConf getInt "window.length.secs"

  type Statistic = mutable.Map[LogMessageLevel.Value, Int]
  import ru.ps.onef.research.kafka.domain.LogMessageLevel._

  /**
    * Function save statistics to HBase
    * Since we didn't have guarantee on cleanup method
    * we should open and close connection on every write,
    *
    * @param timestamp of statistics
    * @param statistics url with counts per level
    */
  private def saveToHbase(timestamp: Long, statistics: mutable.Map[String, Statistic]) = {
    val connection = ConnectionFactory.createConnection(conf)
    val tableName = TableName.valueOf("test_table")
    val table = connection.getTable( tableName )

    try {
      val reverseTimestamps = Long.MaxValue - timestamp

      statistics.foreach { case (url, stat) =>
        val thePut= new Put(Bytes.toBytes(s"${url}_$reverseTimestamps"))

        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("url"),Bytes.toBytes(url))
        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("ts"),Bytes.toBytes(timestamp))
        var total = 0
        stat.foreach { case (level, count) =>
          thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes(level),Bytes.toBytes(count))
          total += count
        }
        thePut.addColumn(Bytes.toBytes("data"),Bytes.toBytes("rate"),Bytes.toBytes(total / windowLength))
        table.put(thePut)
      }
    } finally {
      table.close()
      connection.close()
    }

  }


  override def execute(inputWindow: TupleWindow): Unit = {
    import collection.JavaConverters._

    val minMsgByTs = inputWindow.getNew.asScala.flatMap { sTuple =>
      ConsoleLogsConsumer decodeTuple sTuple
    }.sortBy(_.ts).headOption

    //TODO Rework contract, on processing statistic
    // since in current implementation we skip fading trend
    minMsgByTs map { min => //we will emit messages only if new messages was received
      val statistics: mutable.Map[String, Statistic]  = mutable.Map.empty

      inputWindow.get.asScala flatMap( sTuple => ConsoleLogsConsumer decodeTuple sTuple ) foreach { message =>
        val urlSt = statistics.getOrElseUpdate(message.url, mutable.Map.empty)

        urlSt.update(message.level, urlSt.getOrElseUpdate(message.level, 0) + 1)
      }

      saveToHbase(min.ts, statistics)
      // emit the results
//      collector.emit(new Values(computedValue))
    }
  }

}