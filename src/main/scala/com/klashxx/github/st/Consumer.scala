package com.klashxx.github.st

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType


object Consumer extends App {
  println("Consuming ...")

  val propsMaps = Common.getPropsMaps

  val spark = SparkSession.builder
    .appName(propsMaps.getOrElse("app.name", "evstx"))
    .master(propsMaps.getOrElse("master.mode", "local"))
    .getOrCreate

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", propsMaps.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
    .option("subscribe", propsMaps.getOrElse("topic.name", "evstx"))
    .option("startingOffsets", "latest")
    .load()

  df.printSchema()

  val schema = ScalaReflection.schemaFor[Message].dataType.asInstanceOf[StructType]

  df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    .writeStream
    .format("console")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()
    .awaitTermination()
}
