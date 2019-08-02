package kafka
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import scala.util.Random
import kafka.producer.KeyedMessage
import java.util.Date
object ScalaProducerExample extends App {
  val events = 5
  val topic = "test"
  val brokers = "192.168.11.30:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  //props.put("partitioner.class", "com.colobu.kafka.SimplePartitioner")
  props.put("producer.type", "async")
  //props.put("request.required.acks", "1")
  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime
    val ip = "192.168.2." + rnd.nextInt(255)
    val msg = runtime + "," + nEvents + ",www.example.com," + ip
    val data = new KeyedMessage[String, String](topic, ip, msg)
    producer.send(data)
  }
  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
