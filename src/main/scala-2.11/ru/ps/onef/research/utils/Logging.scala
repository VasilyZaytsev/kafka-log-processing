package ru.ps.onef.research.utils

/**
  * Created by Vasily.Zaytsev on 05.12.2016.
  */
import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging {

  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}
