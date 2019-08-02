import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming {
  def main(args: Array[String]): Unit = {
    test1()
  }
  def test1(): Unit ={
    val conf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = Map("test" -> 1)
    val lines = KafkaUtils.createStream(ssc,"192.168.11.30:2181","test",topics).map(_._2)
    lines.print()
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
  def test2(): Unit ={
    val conf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topicsSet = "test".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "192.168.11.30:9092", "auto.offset.reset" -> "smallest", "group.id" -> "test-2")

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
    val topicAndPartiton = TopicAndPartition("test", 0)
    val lastStopOffset = Map(topicAndPartiton->94L)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, lastStopOffset, messageHandler)

    var offsetRanges = Array[OffsetRange]()
    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
