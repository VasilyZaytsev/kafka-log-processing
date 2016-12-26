package com.whisk.docker.impl.dockerjava

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model._
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.typesafe.config.ConfigFactory
import com.whisk.docker._

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._

/**
  * This fix was develop for only one reason, pass image host from configuration to container cmd creation
  * Such solution is not best idea, but for testing purpose it's fine.
  * Main problem is that configuration restricted by com.whisk.docker.DockerContainer,
  * thus next improvements should eliminate this case class and provide full api of container cmd
  *
  * Created by Vasily.Zaytsev on 19.12.2016.
  */
trait DockerKitWithFix extends DockerKit {

  override implicit val dockerFactory: DockerFactory = new DockerJavaExecutorFactoryFix(
    new Docker(DefaultDockerClientConfig.createDefaultConfigBuilder().build()))

}

class DockerJavaExecutorFactoryFix(docker: Docker) extends DockerJavaExecutorFactory(docker: Docker) {

  override def createExecutor(): DockerCommandExecutor = {
    new DockerJavaExecutorFix(docker.host, docker.client)
  }

}

class DockerJavaExecutorFix(host: String, client: DockerClient)
  extends  DockerJavaExecutor(host, client) {
  private val dockerConfig = ConfigFactory.load()

  override def createContainer(spec: DockerContainer)(
    implicit ec: ExecutionContext): Future[String] = {
    val volumeToBind: Seq[(Volume, Bind)] = spec.volumeMappings.map { mapping =>
      val volume: Volume = new Volume(mapping.container)
      (volume, new Bind(mapping.host, volume, AccessMode.fromBoolean(mapping.rw)))
    }

    val baseCmd = {
      val tmpCmd = client
        .createContainerCmd(spec.image)
        .withPortSpecs(spec.bindPorts
          .map({
            case (guestPort, DockerPortMapping(Some(hostPort), address)) =>
              s"$address:$hostPort:$guestPort"
            case (guestPort, DockerPortMapping(None, address)) => s"$address::$guestPort"
          })
          .toSeq: _*)
        .withExposedPorts(spec.bindPorts.keys.map(ExposedPort.tcp).toSeq: _*)
        .withTty(spec.tty)
        .withStdinOpen(spec.stdinOpen)
        .withEnv(spec.env: _*)
        .withPortBindings(
          spec.bindPorts.foldLeft(new Ports()) {
            case (ps, (guestPort, DockerPortMapping(Some(hostPort), address))) =>
              ps.bind(ExposedPort.tcp(guestPort), Ports.Binding.bindPort(hostPort))
              ps
            case (ps, (guestPort, DockerPortMapping(None, address))) =>
              ps.bind(ExposedPort.tcp(guestPort), Ports.Binding.empty())
              ps
          }
        )
        .withLinks(
          spec.links.map {
            case ContainerLink(container, alias) =>
              new Link(container.name.get, alias)
          }.asJava
        )
        .withVolumes(volumeToBind.map(_._1): _*)
        .withBinds(volumeToBind.map(_._2): _*)
        .withHostName(dockerConfig getString "docker.hbase.image-host" )

      spec.name.map(tmpCmd.withName).getOrElse(tmpCmd)
    }

    val cmd = spec.command.fold(baseCmd)(c => baseCmd.withCmd(c: _*))
    Future(cmd.exec()).map { resp =>
      if (resp.getId != null && resp.getId != "") {
        resp.getId
      } else {
        throw new RuntimeException(
          s"Cannot run container ${spec.image}: ${resp.getWarnings.mkString(", ")}")
      }
    }
  }
}