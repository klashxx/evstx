package com.klashxx.github.st

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Consumer extends App {
  println("Consuming ...")

  val spark = SparkSession.builder.appName("evstx").master("local").getOrCreate

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "quickstart")
    .option("startingOffsets", "earliest")
    .load()

  df.printSchema()

  df.writeStream
    .format("console")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()
    .awaitTermination()
}
