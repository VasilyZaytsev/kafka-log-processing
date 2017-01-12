package ru.ps.onef.research.kafka.app

import scala.reflect.io.Path

/**
  * Created by Vasily.Zaytsev on 12.01.2017.
  */
object LogsGeneratorApp extends App {

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
    s"""{"level":$level,"description":"Event happens on $url at $ts with severity $level.","ts":$ts,"url":$url"""
  }

  val file = Path("dataset.txt").toFile
  //be careful estimated file with current parameters is size 5 GB !!!
  val writer = file.bufferedWriter()
  try{
    (0 to 9000000).iterator
      .map(n => Seq.fill(Levels.random.nextInt(10)){generate(n)}.mkString("[",",","]") )
      .foreach { messages =>
        writer.write(messages)
      }
  } finally {
    writer.close()
  }
}
