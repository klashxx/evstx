package com.klashxx.github.st

import java.net.URL
import scala.io.Source
import scala.util.{Failure, Success, Using}

package object Common {
  def getPropsMaps: Map[String, String] = {
    val properties: URL = getClass.getResource("/config/project.properties")

    val propsMap = Using(Source.fromURL(properties)) { reader =>
      reader
        .getLines()
        .filter(line => line.contains("=") && !line.startsWith("#"))
        .map { line =>
          val confMap = line.split("=")
          if (confMap.size == 1) {
            confMap(0) -> ""
          } else {
            confMap(0) -> confMap(1)
          }
        }.toMap
    }

    propsMap match {
      case Success(value) => value
      case Failure(exception) =>
        println(exception.getMessage)
        Map[String, String]()
    }
  }
}
