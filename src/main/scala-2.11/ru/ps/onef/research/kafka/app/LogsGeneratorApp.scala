package ru.ps.onef.research.kafka.app

import play.api.libs.json.Json
import ru.ps.onef.research.kafka.domain.{LogMessage, LogMessageLevel}
import ru.ps.onef.research.kafka.{ConsoleLogsConsumer, LogsProducer}

import scala.io.{Codec, Source}
import scala.reflect.io.{File, Path}

/**
  * Created by Vasily.Zaytsev on 12.01.2017.
  */
object LogGeneratorConstants {
  class ArrayWithRandom[T](val arr: Array[T]) {
    val random = new scala.util.Random()
    def randomIndex: Int = random.nextInt(arr.length)
    def randomValue: T = arr( randomIndex )
    //fabric method
    def withRandom: ArrayWithRandom[T] = this
  }

  implicit def withRandomConverter[T](arr: Array[T]): ArrayWithRandom[T] = {
    new ArrayWithRandom(arr)
  }

  val Levels = Array(-1, 1, 2, 3, 4, 5).withRandom
  val URLs = Array("www.example.com","www.acme.com","www.company.com","www.fake.com","www.site.com","www.some.com").withRandom
  val _18march2005 = 1111111111L

  def generate(n: Int): String = {
    val ts = _18march2005 + n
    val level = Levels.randomValue
    val url = URLs.randomValue
    val msg = LogMessage(level, s"Event happens on $url at $ts with severity ${LogMessageLevel(level)}.", ts, url)
//    s"""{"level":$level,"description":"Event happens on $url at $ts with severity $level.","ts":$ts,"url":"$url"}"""
    //Using Json library to generate json string is more safe
    //but it increase execution time up to 2.5 times
    Json.stringify(Json.toJson(msg))
  }

  /**
    * File will be created at root folder
    */
  val DefaultDatasetPath = Path("dataset.txt")
}

object LogsGeneratorReaderApp extends App {
  val source = Source.fromFile(LogGeneratorConstants.DefaultDatasetPath.toURI)(Codec.UTF8)

  println(s"Source URI is ${LogGeneratorConstants.DefaultDatasetPath.toURI}")
  println(s"Start reading from source $source")

  source.getLines.take(3) foreach { str =>
    println(s"Read message $str")
    val listMsgsJson = ConsoleLogsConsumer convert Json.parse(str)
    println(s"Parsed message $listMsgsJson")
  }
}

object LogsGeneratorApp extends App {
  import LogGeneratorConstants._

  val file = File(DefaultDatasetPath) (Codec.UTF8)
  //be careful estimated file with current parameters is size 5 GB !!!
  println(s"Start writing generated dataset to ${DefaultDatasetPath.toURI}")
  val writer = file.bufferedWriter
  try{
    (0 to 1000000).iterator
      .map(n => Seq.fill(Levels.random.nextInt(10)){generate(n)}.mkString("[",",","]\n") )
      .foreach { messages =>
        writer.write(messages)
      }
  } finally {
    writer.close()
  }
  println(s"Dataset was written successfully")
}
