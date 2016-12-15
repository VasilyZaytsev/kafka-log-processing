package ru.ps.onef.research.docker

import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import com.whisk.docker.config.DockerTypesafeConfig.DockerConfig
import com.whisk.docker.{DockerContainer, DockerKit}

/**
  * Created by Vasily.Zaytsev on 13.12.2016.
  */
trait DockerHBaseService extends DockerKit {

  def dockerConfig: Config = ConfigFactory.load()

  val hbaseContainer: DockerContainer =
    dockerConfig.as[DockerConfig]("docker.hbase").toDockerContainer()

  abstract override def dockerContainers: List[DockerContainer] = hbaseContainer :: super.dockerContainers

}