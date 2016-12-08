package ru.ps.onef.research.embeded

import com.typesafe.config.ConfigFactory
import org.apache.curator.test.TestingServer
import ru.ps.onef.research.utils.LazyLogging

/**
  * Runs an in-memory, "embedded" instance of a ZooKeeper server.
  * The ZooKeeper server instance is automatically started when you create a new instance of this class.
  *
  * Created by Vasily.Zaytsev on 05.12.2016.
  */
class EZooKeeper extends LazyLogging {

  private val conf = ConfigFactory load()
  private val zooKeeperConf = conf getConfig "infrastructure.zookeeper"
  val port: Int = zooKeeperConf getInt "port"

  logger.debug(s"Starting embedded ZooKeeper server on port $port...")

  private val server = new TestingServer(port)

  /**
    * Stop the instance.
    */
  def stop() {
    logger.debug(s"Shutting down embedded ZooKeeper server on port $port...")
    server.close()
    logger.debug(s"Shutdown of embedded ZooKeeper server on port $port completed")
  }

  /**
    * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
    * Example: `127.0.0.1:2181`.
    *
    * You can use this to e.g. tell Kafka and Storm how to connect to this instance.
    */
  val connectString: String = server.getConnectString

  /**
    * The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
    */
  val hostname: String = connectString.splitAt(connectString lastIndexOf ':')._1

}

object EZooKeeper {
  private val instance = new EZooKeeper
  def apply(): EZooKeeper = instance
}