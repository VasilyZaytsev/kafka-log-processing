package ru.ps.onef.research

import java.util.Properties

import com.typesafe.config.Config

/**
  * Created by Vasily.Zaytsev on 05.12.2016.
  */
package object utils {
  def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }
}
