package com.klashxx.github.st

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.util.Random

object Producer extends App {
  val propsMaps = Common.getPropsMaps

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, propsMaps.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerExample")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val topic = propsMaps.getOrElse("topic.name", "evstx")
  val gson = new Gson
  var counter = 0

  try {
    while (true) {
      val numMessagesPerId = Random.between(2, 6)
      val id = java.util.UUID.randomUUID.toString replaceAll("-", "")
      while (counter <= numMessagesPerId) {
        val message = UserEvent(
          id,
          System.currentTimeMillis / 1000,
          if (counter == numMessagesPerId) true else false,
          ""
        )
        val jsonMessage = gson.toJson(message)
        val record = new ProducerRecord[String, String](topic, id, jsonMessage)
        producer.send(record)
        println(jsonMessage)
        Thread.sleep(1 * 1000)
        counter += 1
      }
      counter = 0
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
