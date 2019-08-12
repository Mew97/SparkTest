package myKafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

object ScalaProducerExample extends  App{
  val topic = "data01"
  val brokers = "cdh1:9092"
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val t = System.currentTimeMillis()
  for (i <- 1 to 3) {
    val msg = s"hello, I'm the $i message!"
    val record = new KeyedMessage[String, String](topic, "key", msg)
    producer.send(record)
  }
  producer.close()

}
