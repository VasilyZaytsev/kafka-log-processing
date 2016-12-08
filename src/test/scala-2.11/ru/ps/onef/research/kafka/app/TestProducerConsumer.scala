package ru.ps.onef.research.kafka.app

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import ru.ps.onef.research.embeded.{EKafka, EZooKeeper}
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.kafka.domain.LogMessageLevel._
import LogsProducer._
import org.apache.storm.{Config, ILocalCluster, Testing}
import org.apache.storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}
import org.apache.storm.testing.{MkClusterParam, TestJob}
import org.apache.storm.topology.TopologyBuilder
import ru.ps.onef.research.storm.SimpleUpdateBolt

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Vasily.Zaytsev on 06.12.2016.
  */
class TestProducerConsumer extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val outputTopicName = "processed-logs"
  val inputTopicName = "raw-logs"
  val stopMessage = LogMessage(DEBUG, "Last message", 0L)

  private var stormConsumerInstance: Option[ConsoleLogsConsumer] = None
  private var consumerInstance: Option[ConsoleLogsConsumer] = None

  override def beforeAll {
    EZooKeeper()
    EKafka().start()

    Seq(LogsProducer.defaultTopic, outputTopicName, inputTopicName)
      .foreach(EKafka().createTopic(_))

    consumerInstance = Some(ConsoleLogsConsumer())
    stormConsumerInstance = Some(ConsoleLogsConsumer(outputTopicName))
  }

  override def afterAll {
    Seq(consumerInstance, stormConsumerInstance).foreach(_.foreach(_.shutdown()))
    EKafka().stop()
    EZooKeeper().stop()
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

      (1 to totalAmount).toList
        .map { n => LogMessage(TRACE, "Message " + n, 0L) }
        .grouped(batchSize).foreach { listMsgs =>
        LogsProducer send listMsgs
      }

      LogsProducer send List(stopMessage)
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

      val stormTestClusterParameters = {
        val mkClusterParam = new MkClusterParam
        mkClusterParam.setSupervisors(2)
        val stormClusterConfig = new Config

        import scala.collection.JavaConverters._
        stormClusterConfig.put(Config.STORM_ZOOKEEPER_SERVERS, List(EZooKeeper().hostname).asJava)
        stormClusterConfig.put(Config.STORM_ZOOKEEPER_PORT, EZooKeeper().port: Integer)

        mkClusterParam.setDaemonConf(stormClusterConfig)
        mkClusterParam
      }

      Testing.withLocalCluster(stormTestClusterParameters, new TestJob() {
        override def run(stormCluster: ILocalCluster) {
          val topologyName = "storm-kafka-integration-test"
          stormCluster.submitTopology(topologyName, topologyConfig, topology)

          val batchSize = 1

          (1 to stormTotalAmount).toList
            .map { n => LogMessage(TRACE, "Message " + n, 0L) }
            .grouped(batchSize)
            .foreach { listMsgs =>
            LogsProducer.send(listMsgs)(inputTopicName)
          }

          LogsProducer.send(List(stopMessage))(inputTopicName)

          //Important NOTE!
          //Next await is important, since if we finish this code before storm process all messages
          //it will cause to test fail!
          stormConsumerResult = Some(Await.result(fResultOpt.get, 10.seconds))
        }
      })
    }

    "get result from Consumer and Result should be expected size" in {
      stormConsumerResult.get should have size stormTotalAmount
      stormConsumerResult.get.foreach { msg =>
        msg.description.split(" ") should contain ("processed")
      }
    }

  }
}
