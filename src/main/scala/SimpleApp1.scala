import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp1 {
  def main(args: Array[String]): Unit = {
    test_sql_1()
  }

  def test1(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/pubg/aggregate")
    distFile
      .map(_.split(","))
      .filter(x=>{x(14)=="1" && (x(4) == "4" || x(4) == "2")})
      .map(x=>(x(2)+x(13), (x(10).toInt, x(5).toInt)))
      .reduceByKey((v1,v2)=>{(v1._1.toInt+v2._1.toInt,v1._2.toInt+v2._2.toInt)})
      .saveAsTextFile("hdfs://CDH1:8020/pubg/zhubo/rs3")
  }

  def test1_1(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/pubg/aggregate")
    val scRdd = distFile
      .map(_.split(","))
      .filter(x => {
        x(14) == "1" && x(4) == "1"
      })
    val avg = scRdd
      .map(x => (1, (x(10).toInt, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(x => (x._2._1 / x._2._2, x._2._2))
      .first()

    val rs = scRdd
      .filter(x=>x(10).toInt>avg._1)
      .count()

    println(rs > avg._2/2)
  }

  def test_sql_1(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet("hdfs://192.168.2.31:8020/user/hive/warehouse/pubg.db/aggregate_p")
    df.registerTempTable("aggregate")
    val df1 =  sqlContext.sql("select avg(cast(player_kills as int)) a, count(*) b from aggregate where team_placement = '1' and party_size = '1'")
    df1.registerTempTable("df1")
    sqlContext.sql("select count(*),cast(player_kills as int) a from aggregate where a > (select a from df1) union select count(*), cast(player_kills as int) b from aggregate where b < (select a from df1)")
      .write.json("hdfs://CDH1:8020/pubg/zhubo/rs2")
  }


  def test2(): Unit ={
    val conf = new SparkConf().setAppName("mySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/user/hive/warehouse/pubg.db/aggregate_t")
    distFile
      .map(_.split(","))
      .filter(x=>{x(14)=="1" && (x(4) == "4" || x(4) == "2")})
      .map(x=>(x(2)+x(13), (x(10).toInt, x(5).toInt)))
      .reduceByKey((v1,v2)=>{(v1._1.toInt+v2._1.toInt,v1._2.toInt+v2._2.toInt)})
      .map(x=>(x._1,x._2._2.toDouble/x._2._1))
      .foreach(println)
  }

  def test2_1(): Unit = {
    val conf = new SparkConf().setAppName("mySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/user/hive/warehouse/pubg.db/aggregate_t")
    println(distFile
      .map(_.split(","))
      .filter(x => {
        x(14) == "1" && x(4) == "1"
      })
      .map(x => (1, (x(10).toInt, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(x => x._2._1 / x._2._1)
      .first())
  }

  def test3(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/pubg/deaths")
    distFile
      .map(_.split(","))
      .filter(x=>{x(0)=="death.None"})
      .map(x=>(x(1),x(8)))
      .saveAsTextFile("hdfs://CDH1:8020/pubg/zhubo/rs6")
  }

  def test4(): Unit ={
    val conf = new SparkConf().setAppName("mySpark").setMaster("local")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("hdfs://CDH1:8020/user/hive/warehouse/pubg.db/deaths_t")
    println(distFile
      .map(_.split(","))
      .filter(x => {
        x(0) == "death.None"
      })
      .map(x=>(x(1),x(8)))
      .first())
  }


}
