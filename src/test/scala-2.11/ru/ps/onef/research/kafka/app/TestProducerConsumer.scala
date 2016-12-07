package ru.ps.onef.research.kafka.app

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import ru.ps.onef.research.embeded.{EKafka, EZooKeeper}
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}
import ru.ps.onef.research.kafka.domain.LogMessage
import ru.ps.onef.research.kafka.domain.LogMessageLevel._
import LogsProducer._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by Vasily.Zaytsev on 06.12.2016.
  */
class TestProducerConsumer extends WordSpecLike with Matchers with BeforeAndAfterAll {

// Run completed in 2 minutes, 59 seconds.
//  val totalAmount = 1000000000 -> java.lang.OutOfMemoryError: GC overhead limit exceeded

  val totalAmount = 10000
  val stopMessage = LogMessage(DEBUG, "Last message", 0L)
  private var consumerInstance: Option[ConsoleLogsConsumer] = None

  override def beforeAll {
    EZooKeeper()
    EKafka().start()
    EKafka().createTopic(LogsProducer.defaultTopic)
    consumerInstance = Some(ConsoleLogsConsumer())
  }

  override def afterAll {
    consumerInstance.foreach(_.shutdown)
    EKafka().stop()
    EZooKeeper().stop()
  }

  "Test" should {
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
}
