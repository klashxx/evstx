package com.klashxx.github.st

import org.apache.spark.sql.SparkSession

object Runner extends App {
  println("Running ...")

  val spark = SparkSession.builder.appName("evstx").master("local").getOrCreate

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "quickstart")
    .option("startingOffsets", "earliest") // From starting
    .load()

  df.printSchema()
}
