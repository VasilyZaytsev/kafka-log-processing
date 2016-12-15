package ru.ps.onef.research.docker

import com.spotify.docker.client.DefaultDockerClient
import com.whisk.docker.{DockerContainer, DockerFactory, DockerKit}
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import org.scalatest.Suite
import com.whisk.docker.scalatest.DockerTestKit
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
  * Created by Vasily.Zaytsev on 14.12.2016.
  */
trait DockerTestKitDockerEnv extends DockerTestKit with DockerKit {
  self: Suite =>

  private lazy val log = LoggerFactory.getLogger(this.getClass)

  private val dcBulider = DefaultDockerClient.fromEnv()

  override implicit val dockerFactory: DockerFactory = {
    val logMsg = s"Crete Docker client from environment variables URI is ${dcBulider.uri.toString}"
    println(logMsg)
    new SpotifyDockerFactory(dcBulider.build())
  }

}
