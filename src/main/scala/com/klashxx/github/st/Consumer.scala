package com.klashxx.github.st

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.StructType


object Consumer extends App {
  val propsMaps = Common.getPropsMaps

  println("Consuming ...")

  val spark = SparkSession.builder
    .appName(propsMaps.getOrElse("app.name", "evstx"))
    .master(propsMaps.getOrElse("master.mode", "local"))
    .getOrCreate

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", propsMaps.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
    .option("subscribe", propsMaps.getOrElse("topic.name", "evstx"))
    .option("startingOffsets", "latest")
    .option("checkpointLocation",  propsMaps.getOrElse("checkpoint.path", "/tmp/evstx-ckeckpoint"))
    .load()

  val schema = ScalaReflection.schemaFor[UserEvent].dataType.asInstanceOf[StructType]


  import spark.implicits._

  def updateSessionEvents(id: String,
                          userEvents: Iterator[UserEvent],
                          state: GroupState[UserSession]): Option[UserSession] = {
    if (state.hasTimedOut) {
      state.remove()
      state.getOption
    } else {
      val currentState = state.getOption
      val updatedUserSession =
        currentState.fold(UserSession(userEvents.toSeq))(currentUserSession =>
          UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
      state.update(updatedUserSession)

      if (updatedUserSession.userEvents.exists(_.isLast)) {
        val userSession = state.getOption
        state.remove()
        userSession
      } else {
        state.setTimeoutDuration("1 minute")
        None
      }
    }
  }

  df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    .selectExpr("id", "CAST(timestamp AS TIMESTAMP)", "isLast", "data")
    .as[UserEvent]
    .groupByKey(_.id)
    .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(
      updateSessionEvents)
    .flatMap(userSession => userSession)
    .writeStream
    .outputMode(OutputMode.Update())
    .queryName("evstx")
    .format("console")
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .start()
    .awaitTermination()
}
