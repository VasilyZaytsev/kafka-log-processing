package ru.ps.onef.research.embeded

import java.io.File
import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZKStringSerializer
import org.apache.commons.io.FileUtils
import ru.ps.onef.research.utils.LazyLogging

/**
  *
  * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by default.
  *
  * Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper instance running at
  * `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting the `zookeeper.connect` parameter in the
  * broker's configuration.
  *
  * @param config Broker configuration settings.  Used to modify, for example, on which port the broker should listen to.
  *               Note that you cannot change the `log.dirs` setting currently.
  *
  * Created by Vasily.Zaytsev on 05.12.2016.
  */
class EKafka(config: Properties = new Properties) extends LazyLogging {

  private val conf = ConfigFactory load()
  private val kafkaConf = conf getConfig "infrastructure.kafka"

//  private val defaultZkConnect = s"${kafkaConf getString "host.name"}:${kafkaConf getInt "port"}"
//  private val logDir = {
//    val random = (new scala.util.Random).nextInt()
//    val path = Seq(System.getProperty("java.io.tmpdir"), "kafka-test", "logs-" + random).mkString(File.separator)
//    new File(path)
//  }
//
//  private val effectiveConfig = {
//    val props = propsFromConfig(kafkaConf)
//    props.setProperty("log.dirs", logDir.getAbsolutePath)
//    props
//  }
//
//  private val kafkaConfig = new KafkaConfig(effectiveConfig)
//  private val kafka = new KafkaServerStartable(kafkaConfig)
//
//  /**
//    * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
//    *
//    * You can use this to tell Kafka producers and consumers how to connect to this instance.
//    */
//  val brokerList = kafka.serverConfig.hostName + ":" + kafka.serverConfig.port
//
//  /**
//    * The ZooKeeper connection string aka `zookeeper.connect`.
//    */
//  val zookeeperConnect = {
//    val zkConnectLookup = Option(effectiveConfig.getProperty("zookeeper.connect"))
//    s"${kafkaConf getString "host.name"}:${kafkaConf getInt "port"}"
//    zkConnectLookup match {
//      case Some(zkConnect) => zkConnect
//      case _ =>
//        logger.warn(s"zookeeper.connect is not configured -- falling back to default setting $defaultZkConnect")
//        defaultZkConnect
//    }
//  }
//
//  /**
//    * Start the broker.
//    */
//  def start() {
//    logger.debug(s"Starting embedded Kafka broker at $brokerList (with ZK server at $zookeeperConnect) ...")
//    kafka.startup()
//    logger.debug(s"Startup of embedded Kafka broker at $brokerList completed (with ZK server at $zookeeperConnect)")
//  }
//
//  /**
//    * Stop the broker.
//    */
//  def stop() {
//    logger.debug(s"Shutting down embedded Kafka broker at $brokerList (with ZK server at $zookeeperConnect)...")
//    kafka.shutdown()
//    FileUtils.deleteQuietly(logDir)
//    logger.debug(s"Shutdown of embedded Kafka broker at $brokerList completed (with ZK server at $zookeeperConnect)")
//  }
//
//  def createTopic(topic: String, partitions: Int = 1, replicationFactor: Int = 1, config: Properties = new Properties): Unit = {
//    logger.debug(s"Creating topic { name: $topic, partitions: $partitions, replicationFactor: $replicationFactor, config: $config }")
//    val sessionTimeout = 10.seconds
//    val connectionTimeout = 8.seconds
//    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then createTopic() will only
//    // seem to work (it will return without error).  Topic will exist in only ZooKeeper, and will be returned when
//    // listing topics, but Kafka itself does not create the topic.
//    val zkClient = new ZkClient(zookeeperConnect, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt,
//      ZKStringSerializer)
//    AdminUtils.createTopic(zkClient, topic, partitions, replicationFactor, config)
//    zkClient.close()
//  }

}