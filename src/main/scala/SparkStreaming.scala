import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming {
  def main(args: Array[String]): Unit = {
    test2()
  }
  def test1(): Unit ={
    val conf = new SparkConf().setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = Map("test1" -> 1)
    val lines = KafkaUtils.createStream(ssc,"cdh1:2181","test-1",topics).map(_._2)
    lines.print()
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
  def test2(): Unit ={
    val conf = new SparkConf().setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("hdfs://cdh1:8020/checkpoint")
//    val topicsSet = "test1".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "cdh1:9092,cdh3:9092,cdh4:9092,cdh5:9092", "group.id" -> "test-2")

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
    //val topicAndPartiton = TopicAndPartition("test3", 1)
    val lastStopOffset = Map(TopicAndPartition("data01", 0)->0L)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, lastStopOffset, messageHandler)

    var offsetRanges = Array[OffsetRange]()



//    def updateFun(newValue: Seq[Int],runningCount: Option[Int]) = {
//      val i = runningCount.getOrElse(0)
//      val j = newValue.sum
//      Some(i + j)
//    }
//    directKafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFun)
//        .foreachRDD(rdd=>{
//          val host = "cdh4"
//          val port = 6379
//          val timeout = 10000
//          val jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout)
//          val jedis = jedisPool.getResource
//          rdd.collect.foreach(x=>jedis.set("zb:"+x._1,x._2.toString))
//          jedis.close()
//        })

    //val DS = directKafkaStream.window(Seconds(30),Seconds(6))

    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd.foreach(println)
//      val tuples = rdd.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).collect()
//      println(tuples.mkString(""))
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
