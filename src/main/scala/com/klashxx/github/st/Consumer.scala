package com.klashxx.github.st

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTimeTimeout
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupState, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType


object Consumer extends App {
  val logger = Logger.getLogger("org")
  logger.setLevel(Level.WARN)

  val propsMaps = Common.getPropsMaps

  println("Consuming ...")

  val spark = SparkSession.builder
    .appName(propsMaps.getOrElse("app.name", "evstx"))
    .master(propsMaps.getOrElse("master.mode", "local"))
    .getOrCreate

  val eventsStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", propsMaps.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
    .option("subscribe", propsMaps.getOrElse("topic.name", "evstx"))
    .option("startingOffsets", "latest")
    .option("checkpointLocation", propsMaps.getOrElse("checkpoint.path", "/tmp/evstx-checkpoint"))
    .load()

  val schema = ScalaReflection.schemaFor[UserEvent].dataType.asInstanceOf[StructType]

  import spark.implicits._

  def updateConnectionState(
                             id: String,
                             events: Iterator[UserEvent],
                             session: GroupState[UserSession]): Option[UserSession] = {

    if (session.hasTimedOut) {
      logger.warn("session-timeout")
      val sessionState = session.getOption
      session.remove()
      sessionState
    } else if (session.exists) {
      logger.warn("existing-user-session: " + id)
      val userEvents = session.get.userEvents ++ events.toSeq
      session.update(UserSession(id, userEvents))

      val sessionState = session.getOption

      if (userEvents.exists(_.isLast == true)) {
        logger.warn("existing-user-session-is-last: " + id)
        session.remove()
        sessionState
      } else {
        logger.warn("existing-user-session-setting-timeout: " + id)
        session.setTimeoutDuration("1 hour")
        None
      }
    } else {
      val userEvents = events.toSeq

      session.update(UserSession(id, userEvents))

      if (userEvents.exists(_.isLast == true)) {
        logger.warn("new-user-session-is-last: " + id)
        val sessionState = session.getOption
        session.remove()
        sessionState
      } else {
        logger.warn("new-user-session: " + id)
        session.setTimeoutDuration("1 hour")
        None
      }
    }
  }

  eventsStream.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    .selectExpr("id", "CAST(timestamp AS TIMESTAMP)", "isLast", "data")
    .as[UserEvent]
    .withWatermark("timestamp", "5 minutes")
    .groupByKey(_.id)
    .mapGroupsWithState(ProcessingTimeTimeout)(updateConnectionState)
    .flatMap(userSession => userSession)
    .writeStream
    .outputMode(OutputMode.Update)
    .queryName("evstx")
    .format("console").option("truncate", "false")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()
    .awaitTermination()
}
