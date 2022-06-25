package com.klashxx.github.st

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

case class Message(id: String, timestamp: Long)

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

  try {
    while(true) {
      val id = java.util.UUID.randomUUID.toString replaceAll("-", "")
      val message = Message(id, System.currentTimeMillis / 1000)
      val jsonMessage = gson.toJson(message)
      val record = new ProducerRecord[String, String](topic, id, jsonMessage)
      producer.send(record)
      println(jsonMessage)
      Thread.sleep(1 * 1000)
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
