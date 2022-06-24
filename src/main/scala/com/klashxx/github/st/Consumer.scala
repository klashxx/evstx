package com.klashxx.github.st

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Consumer extends App {
  println("Consuming ...")

  val spark = SparkSession.builder
    .appName("evstx")
    .master("local")
    .getOrCreate

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "evstx")
    .option("startingOffsets", "latest")
    .load()

  df.printSchema()

  val schema = new StructType()
    .add("id", StringType)
    .add("timestamp", IntegerType)

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
