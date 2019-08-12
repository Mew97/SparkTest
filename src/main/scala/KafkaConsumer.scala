import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._

object KafkaConsumer {

  def cunsumer(): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.11.30:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    props.put("auto.offset.reset","earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe("test")
    while (true){
      val records = consumer.poll(100)
      for (record <- records){
        println(record.toString())
      }
    }
    consumer.close()
  }


  def producer(): Unit ={
    val brokers_list = ""
    val topic = "test"
    val properties = new Properties()
    properties.put("group.id", "jaosn_")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName) //key的序列化;
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)//value的序列化;
    val producer = new KafkaProducer[String, String](properties)
    var num = 0
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    producer()
  }

}
