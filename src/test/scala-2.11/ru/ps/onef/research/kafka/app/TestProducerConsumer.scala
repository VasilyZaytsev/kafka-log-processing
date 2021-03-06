package ru.ps.onef.research.kafka.app

import java.time.LocalDateTime
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import ru.ps.onef.research.embeded.{EKafka, EZooKeeper}
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.kafka.domain.LogMessageLevel._
import LogsProducer._
import com.typesafe.config.ConfigFactory
import com.whisk.docker.impl.dockerjava.DockerKitWithFix
import com.whisk.docker.scalatest.DockerTestKit
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.storm.kafka.trident.{TransactionalTridentKafkaSpout, TridentKafkaConfig, TridentKafkaUpdater}
import org.apache.storm.{Config, LocalCluster}
import org.apache.storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.topology.base.BaseWindowedBolt
import org.apache.storm.trident.TridentTopology
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory
import org.apache.storm.tuple.Fields
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Second, Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json
import ru.ps.onef.research.docker.DockerHBaseService
import ru.ps.onef.research.storm.{AlertingWindowAgg, LogsSWindowAgg, SimpleUpdateBolt}

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.{Codec, Source}

/**
  * Created by Vasily.Zaytsev on 06.12.2016.
  */
class TestProducerConsumer extends WordSpecLike
  with Matchers with BeforeAndAfterAll with ScalaFutures
  with DockerKitWithFix with DockerTestKit with DockerHBaseService {

  import TestProducerConsumer._
  private val logResults: Logger = LoggerFactory.getLogger("storm.test.results.logger")
  private val config = ConfigFactory load()
  private val stormConf = config getConfig "log.storm"
  private val dockerConf = config getConfig "docker.hbase"
  private val alertTopicName = stormConf getString "alert.out.topic.name"

  private var stormConsumerInstance: Option[ConsoleLogsConsumer] = None
  private var alertConsumerInstance: Option[ConsoleLogsConsumer] = None
  private var consumerInstance: Option[ConsoleLogsConsumer] = None

  private val hbaseConfig = {
    val conf = HBaseConfiguration.create()
    import HConstants._
    conf.set(ZOOKEEPER_QUORUM, dockerConf getString "image-host")
    conf
  }

  override def dockerInitPatienceInterval =
    PatienceConfig(scaled(Span(30, Seconds)), scaled(Span(10, Millis)))

  override val StartContainersTimeout: FiniteDuration = 90.seconds

  private lazy val localStormCluster = {
    new LocalCluster(EZooKeeper().hostname, EZooKeeper().port.toLong)
  }

  override def beforeAll {
    super.beforeAll()
    EZooKeeper()
    EKafka().start()

    Seq(LogsProducer.defaultTopic, outputTopicName, inputTopicName, alertTopicName)
      .foreach(EKafka().createTopic(_))

    consumerInstance = Some(ConsoleLogsConsumer())
    stormConsumerInstance = Some(ConsoleLogsConsumer(outputTopicName))
    alertConsumerInstance = Some(ConsoleLogsConsumer(alertTopicName))
  }

  override def afterAll {
    localStormCluster.shutdown()
    ConnectionFactory.createConnection(hbaseConfig).getAdmin.shutdown()
    Seq(consumerInstance, stormConsumerInstance, alertConsumerInstance).foreach(_.foreach(_.shutdown()))
    EKafka().stop()
    EZooKeeper().stop()
    super.afterAll()
  }

  implicit val pc = PatienceConfig(Span(30, Seconds), Span(1, Second))

  "Docker test" should {
    "Start all containers and await until they will be ready" in {
      dockerContainers.map(_.image).foreach(println)
      dockerContainers.forall(isContainerReady(_).futureValue) shouldBe true
    }
  }

  "Hbase test" should {
    "create connection and execute CRUD" in {
      HBaseAdmin.checkHBaseAvailable(hbaseConfig)
      val connection = ConnectionFactory.createConnection(hbaseConfig)
      val admin = connection.getAdmin

      val tableName = TableName.valueOf("test_table")
      if (admin.tableExists(tableName)){
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      val tableDesc = new HTableDescriptor(tableName)
      val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("ids"))
      tableDesc.addFamily(idsColumnFamilyDesc)
      admin.createTable(tableDesc)

      // let's insert some data in 'mytable' and get the row
      val table = connection.getTable( tableName )

      val putValue = "one1"
      for (index <- 1 to 3){
        val thePut= new Put(Bytes.toBytes(s"rowkey$index"))
        thePut.addColumn(Bytes.toBytes("ids"),Bytes.toBytes(s"id$index"),Bytes.toBytes(s"one$index"))
        table.put(thePut)
      }

      val theGet = new Get(Bytes.toBytes("rowkey1"))
      val result = table.get(theGet)
      val getValue = Bytes.toString(result.value)

      table.close()
      connection.close()

      getValue shouldEqual putValue
    }
  }

  "Kafka test" should {
    // Run completed in 2 minutes, 59 seconds.
    //  val totalAmount = 1000000000 -> java.lang.OutOfMemoryError: GC overhead limit exceeded

    val totalAmount = 10000

    var fResultOpt: Option[Future[List[LogMessage]]] = None

    "create Consumer and read messages from topic until gets stop message" in {
      fResultOpt = Some(Future {
        consumerInstance.get.read
          .takeWhile { !_.headOption.exists(_ == stopMessage) }
          .flatten
          .toList
      })
    }

    "create Producer and send message to topic" in {
      val batchSize = 100

      (1 to totalAmount)
        .map { n => LogMessage(TRACE, "Message " + n, 0L) }
        .grouped(batchSize).foreach { listMsgs =>
        LogsProducer sendSeq listMsgs
      }

      LogsProducer send stopMessage
    }

    "get result from Consumer and Result should be expected size" in {
      val result = Await.result(fResultOpt.get, 3.seconds)
      result should have size totalAmount
    }
  }

  "Storm test" should {
    val stormTotalAmount = 10
    var fResultOpt: Option[Future[List[LogMessage]]] = None
    var stormConsumerResult: Option[List[LogMessage]] = None

    def kafkaSpoutBaseConfig(zookeeperConnect: String, inputTopic: String): SpoutConfig = {
      val zkRoot = "/kafka-storm-starter-spout"
      val zkId = "kafka-spout"

      val spoutConfig = new SpoutConfig(new ZkHosts(zookeeperConnect), inputTopic, zkRoot, zkId)
      spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime
      spoutConfig
    }

    "create Consumer and read messages from topic until gets stop message" in {
      fResultOpt = Some(Future {
        stormConsumerInstance.get.read
          .takeWhile { !_.headOption.exists(_.description.contains(stopMessage.description)) }
          .flatten
          .toList
      })
    }

    "build topology and depoloy it on local cluster" in {
      val builder = new TopologyBuilder
      val kafkaSpoutId = "kafka-spout"
      val kafkaSpoutConfig = kafkaSpoutBaseConfig(EZooKeeper().connectString, inputTopicName)
      val kafkaSpout = new KafkaSpout(kafkaSpoutConfig)

      val numSpoutExecutors = 1
      builder.setSpout(kafkaSpoutId, kafkaSpout, numSpoutExecutors)

      val kafkaSinkBoltId = "kafka-sink-bolt"
      val kafkaSinkBolt = new SimpleUpdateBolt(outputTopicName)
      builder.setBolt(kafkaSinkBoltId, kafkaSinkBolt).globalGrouping(kafkaSpoutId)
      val topology = builder.createTopology()

      val topologyConfig = {
        val conf = new Config
        conf.setNumWorkers(2)
        conf
      }

      val topologyName = "storm-kafka-integration-test"
      localStormCluster.submitTopology(topologyName, topologyConfig, topology)

      val batchSize = 1

      (1 to stormTotalAmount)
        .map { n => LogMessage(TRACE, "Message " + n, 0L) }
        .grouped(batchSize)
        .foreach { listMsgs =>
          LogsProducer.sendSeq(listMsgs)(inputTopicName)
        }

      LogsProducer.send(stopMessage)(inputTopicName)

      //Important NOTE!
      //Next await is important, since if we finish this code block before storm process all messages
      //it will cause to test fail!
      stormConsumerResult = Some(Await.result(fResultOpt.get, 10.seconds))

      localStormCluster.killTopology(topologyName)
    }

    "get result from Consumer and Result should be expected size" in {
      stormConsumerResult.get should have size stormTotalAmount
      stormConsumerResult.get.foreach { msg =>
        msg.description.split(" ") should contain ("processed")
      }
    }

  }

  "Trident topology test" should {
    val hbaseTableName = stormConf getString "table.name"
    val windowLength = stormConf getInt "window.length.secs"
    val windowSlideLength = stormConf getInt "window.slide.secs"
    val alertConditionCount = stormConf getInt "alert.condition.count"
    val alertsAmountPerWindow = alertConditionCount + 1
    val windowLengthDuration = new BaseWindowedBolt.Duration(windowLength, TimeUnit.SECONDS)
    val windowSlideDuration = new BaseWindowedBolt.Duration(windowSlideLength, TimeUnit.SECONDS)
    val alertWindowLengthDuration = new BaseWindowedBolt.Duration(stormConf getInt "window.alert.length.secs", TimeUnit.SECONDS)
    val tridentTopologyName = "storm-kafka-trident-topology-test"

    def kafkaSpoutBaseConfig(inputTopic: String) = {
      val spoutConfig = new TridentKafkaConfig(new ZkHosts(EZooKeeper().connectString), inputTopic)
      spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime
      spoutConfig
    }

    def recreateHBaseTestTable(): Unit = {
      HBaseAdmin.checkHBaseAvailable(hbaseConfig)
      val conn = ConnectionFactory.createConnection(hbaseConfig)
      try {
        val admin = conn.getAdmin
        val tableName = TableName.valueOf(hbaseTableName)

        if (admin.tableExists(tableName)){
          admin.disableTable(tableName)
          admin.deleteTable(tableName)
        }
        val tableDesc = new HTableDescriptor(tableName)
        val columnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("data"))
        tableDesc.addFamily(columnFamilyDesc)
        admin.createTable(tableDesc)
      }
      finally {
        conn.close()
      }
    }

    "check HBase availability and recreate table, if it exist" in {
      recreateHBaseTestTable()
    }

    "build topology and deploy it on local cluster" in {
      val topology = new TridentTopology()
      val kafkaSpoutId = "kafka-trident-spout"
      val kafkaSpoutConfig = kafkaSpoutBaseConfig(inputTopicName)
      val kafkaSpoutSliding = new TransactionalTridentKafkaSpout(kafkaSpoutConfig)
      val kafkaSpoutTumbling = new TransactionalTridentKafkaSpout(kafkaSpoutConfig)

      topology.newStream(s"$kafkaSpoutId-slidingWindow", kafkaSpoutSliding)
         .slidingWindow(
          windowLengthDuration,
          windowSlideDuration,
          new InMemoryWindowsStoreFactory(),
          new Fields("bytes"),
          new LogsSWindowAgg("bytes", onCompleteCallback ),
          new Fields("bytes")
        )

//      val producerProps = new Properties()
//      producerProps.put("bootstrap.servers", "192.168.173.131:9092")

      //TODO Implement -> 1. TridentKafkaUpdater
      topology.newStream(s"$kafkaSpoutId-tumblingWindow", kafkaSpoutTumbling)
        .tumblingWindow(
           alertWindowLengthDuration,
           new InMemoryWindowsStoreFactory(),
           new Fields("bytes"),
           new AlertingWindowAgg("bytes"),
           new Fields("bytes")
         )

//        .partitionPersist(
//          stateFactory,
//          StreamField.of(StreamField.LEVEL, StreamField.ERROR_JSON),
//          new TridentKafkaUpdater())
//      withProducerProperties(producerProps)
//        .withKafkaTopicSelector(new DefaultTopicSelector("alert-topic"))
//        .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(StreamField.LEVEL_STR, StreamField.ERROR_JSON_STR));


      val topologyConfig = {
        val conf = new Config
        conf
      }

      localStormCluster.submitTopology(tridentTopologyName, topologyConfig, topology.build())

      val stormTotalAmount = 100000
      val batchSize = stormTotalAmount / 10
      val _18march2005 = 1111111111L

      (1 to stormTotalAmount)
        .map { n => LogMessage(TRACE, "Message " + n, _18march2005 + n) }
        .grouped(batchSize)
        .foreach { listMsgs =>

          val list = listMsgs ++ List.fill(alertsAmountPerWindow)(LogMessage(ERROR, "ERROR Message " + stormTotalAmount, listMsgs.head.ts))
          LogsProducer.sendSeq(list)(inputTopicName)

          Await.result( Future {
            stormConsumerInstance.get.read
              .filter( _.headOption.exists("Window completed" == _.description) )
              .take(1)
              .foreach{msg => logResults.info(msg.toString)}
          }, (windowLength + 1).seconds)
        }

      //Wait until current window will empty
      Thread.sleep( (windowLength + windowSlideLength).seconds.toMillis )
    }

    def prettifyHBaseRow(row: Result): String = {
      s"Key ${Bytes.toString(row.getRow)}${row.rawCells.map{ cell =>
        val family = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
        val qualifier = f"${Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)}%10s"
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        s"\ncf[$family].cq[$qualifier] = [$value]"
      }.mkString
      }"
    }

    "get result from HBase and Result should be as expected" in {
      import collection.JavaConverters._

      val conn = ConnectionFactory.createConnection(hbaseConfig)
      val tableName = TableName.valueOf(hbaseTableName)
      val table = conn getTable tableName

      val scan = new Scan()
      val rs = table.getScanner(scan)
      try {
        val result = rs.asScala.map(prettifyHBaseRow).toSeq

        result should have size windowLength
        result.head shouldBe List(
          "Key www.example.com_9223372035743564696",
          "\ncf[data].cq[     ERROR] = [2]",
          "\ncf[data].cq[     TRACE] = [10000]",
          "\ncf[data].cq[      rate] = [1000.2]",
          "\ncf[data].cq[        ts] = [1111211111]",
          "\ncf[data].cq[       url] = [www.example.com]").mkString

      } finally {
        rs.close()  // always close the ResultScanner!
        table.close()
        conn.close()
      }

      val alertsResult = Await.result( Future {
        alertConsumerInstance.get.read
          .take(windowLength)
          .flatten
          .toList
      }, 3.seconds)
      alertsResult.foreach { alert =>
        alert.description.toInt should be(alertsAmountPerWindow)
      }
    }

    "Hard test: check HBase availability and recreate table, if it exist" in {
      recreateHBaseTestTable()
    }

    "HARD test: execute" in {
      val source = Source.fromFile(LogGeneratorConstants.DefaultDatasetPath.toURI)(Codec.UTF8)
      var counter = 0
      var lastTimesatmp = 0L

      val start = LocalDateTime.now()
      logResults.info(s"HARD Start sending at $start")

      source.getLines foreach { str =>
        val listMsgsJson = ConsoleLogsConsumer convert Json.parse(str)
        LogsProducer.sendSeq(listMsgsJson)(inputTopicName)

        if (listMsgsJson.nonEmpty) {
          val currentMax = listMsgsJson.maxBy(_.ts).ts
          lastTimesatmp = if (currentMax > lastTimesatmp) currentMax else lastTimesatmp
        }
        counter += listMsgsJson.size
      }

      import java.time._
      val end = LocalDateTime.now()

      logResults.info(s"Complete sending at $end seconds taked ${Duration.between(start, end).getSeconds}")
      logResults.info(s"Total number of messages $counter")

      counter shouldBe 4504736

      //Wait until current window will empty
      Thread.sleep( (windowLength + windowSlideLength).seconds.toMillis )
      localStormCluster.killTopology(tridentTopologyName)
    }

    "HARD test: check results" in {
      //Note: This test do not perform any check's since general purpose of this test is a
      //will this test execute at all and get execution statistics
      import collection.JavaConverters._

      val conn = ConnectionFactory.createConnection(hbaseConfig)
      val tableName = TableName.valueOf(hbaseTableName)
      val table = conn getTable tableName

      val scan = new Scan()
      val rs = table.getScanner(scan)
      try {
        val statistic: mutable.Map[String, (Double, Int)] = mutable.Map.empty
        var windowsCount = 0

        val result = rs.asScala.foreach { row =>
          val urlCell = row.getColumnLatestCell(Bytes.toBytes("data"),Bytes.toBytes("url"))
          val rateCell = row.getColumnLatestCell(Bytes.toBytes("data"),Bytes.toBytes("rate"))
          val url = Bytes.toString(urlCell.getValueArray, urlCell.getValueOffset, urlCell.getValueLength)
          val rate: Double = Bytes.toString(rateCell.getValueArray, rateCell.getValueOffset, rateCell.getValueLength).toDouble

          val rateSum: Double = statistic.getOrElse(url, (0D,0))._1 + rate
          val counter = statistic.getOrElse(url, (0D,0))._2 + 1
          statistic.update(url,  (rateSum, counter))

          windowsCount += 1
        }

        val stat = statistic.mapValues {case (rateSum, counter) => rateSum / counter}
        logResults.info(s"Total number of windows is $windowsCount average total rate ${stat.values.sum}")
        stat.toIterator.foreach(stat => logResults.info(stat.toString))

      } finally {
        rs.close()  // always close the ResultScanner!
        table.close()
        conn.close()
      }
    }

  }

}

object TestProducerConsumer {
  val outputTopicName = "processed-logs"
  val inputTopicName = "raw-logs"

  val stopMessage = LogMessage(DEBUG, "Last message", 0L)
  private val logResults: Logger = LoggerFactory.getLogger("storm.test.results.logger.callback")

  def onCompleteCallback(timestamp: Long, statistics: Map[String, LogsSWindowAgg.Statistic]): Unit = {
    logResults.info(s"Complete window with timestamp $timestamp statistics: $statistics")
    val completeMsg = LogMessage(DEBUG, "Window completed", timestamp)
    LogsProducer.send(completeMsg)(outputTopicName)
  }
}
