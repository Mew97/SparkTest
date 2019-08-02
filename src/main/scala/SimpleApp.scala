import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

//  def main(args: Array[String]) {
//    val logFile = "/Users/zhubo/Desktop/ngrok.txt" // Should be some file on your system
//    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    spark.stop()
//  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    val spark = new SparkContext(conf)
//    val distFile = spark.textFile(logFile)
//    val lineLengths = distFile.flatMap(_.split(" ")).map(s=>(s,1))
//    val totalLength = lineLengths.reduceByKey((a, b) => a + b)
//    totalLength.collect.foreach(println)
    test6(spark)

//    val list = Seq("hello", "world", "apple", "sleep")
//    list.flatMap(_.split(""))
//      .groupBy(s=>s)
//      .map(m => (m._1,m._2.length))
//      .foreach(println)
//
//    println("=======")
//
//    list.flatMap(_.split("")).foldLeft(Map[String,Int]()){
//      (m,c) => m + (c -> (m.getOrElse(c, 0)+1))
//    }.foreach(println)
//
//    println("=======")

  }

  def test(sc: SparkContext): Unit ={
    val m = Array(("a",2),("b",4),("c",1),("d",3))
    val scRDD = sc.parallelize(m,1)
    val value = scRDD.coalesce(4,shuffle = true)
    println(value.dependencies)
//    val unit = scRDD.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1))
//    unit.foreach(println)

    //unit.first()
    //value2.map(x => (x._2, x._1)).foreach(println)

  }

  def test2(sc: SparkContext): Unit ={
    val aRDD = sc.parallelize(Array(("a",4),("b",2),("c",1),("d",3)),4)
    val bRDD = sc.parallelize(Array(("a",4),("b",2),("c",1),("d",3)),4)
    val unit = aRDD.join(bRDD)
    println(unit.dependencies)
  }

  def test1(sc: SparkContext): Unit ={
    val scRDD = sc.textFile("hdfs://192.168.2.31:8020/test/test1.txt")
    val scRDD1 = scRDD.map(_.split(" ")).cache()
    val people = scRDD1.map(_ (1)).distinct().count()
    println(s"参考人数：$people")
    val people1 = scRDD1.map(x => (x(1),x(2))).filter(x => x._2.toInt < 20).distinct().count()
    println(s"小于20岁参考人数: $people1")
    val people2 = scRDD1.map(x => (x(1),x(2))).filter(x => x._2.toInt == 20).distinct().count()
    println(s"小于20岁参考人数: $people2")
    val people3 = scRDD1.map(x => (x(1),x(2))).filter(x => x._2.toInt > 20).distinct().count()
    println(s"小于20岁参考人数: $people3")
    println("==================")
    val boy = scRDD1.filter(_(3)=="男").map(_(1)).distinct.count
    println(s"一共 $boy 个男生参加考试")
    val girl = scRDD1.filter(_(3)=="女").map(_(1)).distinct.count
    println(s"一共 $girl 个女生参加考试")
    val class_count = scRDD1.map(x => (x(0),x(1))).groupByKey().map(x => (x._1,x._2.toSet.size))
    println(s"12班${class_count.filter(_._1 == "12").map(_._2).first()}人参加考试")
    println(s"13班${class_count.filter(_._1 == "13").map(_._2).first()}人参加考试")
    println("==================")
    val sum = scRDD1.filter(_(4)=="chinese").map(_(5).toDouble).reduce(_+_)
    val count = scRDD1.filter(_(4)=="chinese").count()
    println(s"语文平均分为：${sum/count}")
    val sum1 = scRDD1.filter(_(4)=="math").map(_(5).toDouble).reduce(_+_)
    val count1 = scRDD1.filter(_(4)=="math").count()
    println(s"数学平均分为：${sum1/count1}")
    val sum2 = scRDD1.filter(_(4)=="english").map(_(5).toDouble).reduce(_+_)
    val count2 = scRDD1.filter(_(4)=="english").count()
    println(s"英语平均分为：${sum2/count2}")
    println("=============")
    scRDD1.map(x => (x(1),x(5).toDouble)).groupByKey().map(x => (x._1,x._2.sum/x._2.size))
      .foreach(x => println(s"${x._1}平均分为${x._2}"))
    println("=============")
    scRDD1.filter(x => {x(0)=="12" && x(3)=="男"}).map(x => (x(0),x(5).toDouble)).groupByKey().map(x => x._2.sum/x._2.size)
      .foreach(x => println(s"12班男生平均成绩为$x"))
    scRDD1.filter(x => {x(0)=="12" && x(3)=="女"}).map(x => (x(0),x(5).toDouble)).groupByKey().map(x => x._2.sum/x._2.size)
      .foreach(x => println(s"12班女生平均成绩为$x"))
    println("============")
    scRDD1.filter(_(4)=="chinese").map(x => (x(4),x(5).toDouble)).groupByKey()
      .foreach(x => println(s"语文成绩最高：${x._2.max}"))
    scRDD1.filter(x => x(4)=="chinese" && x(0) == "12").map(x => (x(4),x(5).toDouble)).groupByKey()
      .foreach(x => println(s"12班语文成绩最低：${x._2.min}"))
    scRDD1.filter(x => x(4)=="math" && x(0) == "13").map(x => (x(4),x(5).toDouble)).groupByKey()
      .foreach(x => println(s"13班数学成绩最高：${x._2.max}"))
    val over150 = scRDD1.filter(x => x(0)=="12" && x(3) == "女").map(x=>(x(1),x(5).toDouble)).reduceByKey(_+_)
      .filter(_._2>150).count()
    println(s"总成绩大于150分的12班的女生有${over150}个")






  }

  def test3(sc: SparkContext): Unit ={
    val distFile = sc.textFile("hdfs://192.168.2.31:8020/test/test1.txt",1)
    val lineLengths = distFile.flatMap(_.split(" ")).map(s=>(s,1))
    lineLengths.reduceByKey((a, b) => a + b).collect.foreach(println)
  }

  def test4(sc: SparkContext): Unit ={
    val distFile = sc.textFile("hdfs://192.168.2.31:8020/lol/games.csv")
    val RDD = distFile.mapPartitionsWithIndex{(idx, iter) => if (idx==0) iter.drop(1) else iter}
    println(RDD.first())
  }


  def test5(sc: SparkContext): Unit ={
    val distFile = sc.textFile("/Users/zhubo/Desktop/111.txt",1)
    val RDD = distFile.map(line => {
      val x = line.split(",")
      (new SecondSortByKey(x(0).toInt,x(1).toInt),line)
    })
    RDD.foreach(println)

    RDD.sortByKey(true).map(_._2).foreach(println)
  }
  case class People(name: String,age: Int)
  def test6(sc: SparkContext): Unit ={
    //val distFile = sc.textFile("/Users/zhubo/Desktop/111.txt")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.parquet("hdfs://192.168.2.31:8020/user/hive/warehouse/pubg.db/aggregate_p")
    df.registerTempTable("aggregate")
    sqlContext.sql("select player_assists, count(*) from aggregate where party_size = 4 group by player_assists")
      .show()
//    sqlContext.sql("select distinct(killed_by) from deaths_p")
//      .registerTempTable("a")
//    sqlContext.sql("select count(*) from a")
//      .show()
//    val df = distFile.map(_.split(",")).map(p => People(p(0), p(1).trim.toInt)).toDF()
//    df.registerTempTable("people")
//    df.show()
  }

  def test7(sc: SparkContext): Unit ={
    val distFile = sc.textFile("hdfs://192.168.2.31:8020/pubg/aggregate")
    val scRDD = distFile.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val count = scRDD.map(_.split(",")(13)).distinct.count
    print(count)
  }

  def test8(sc: SparkContext): Unit ={
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val frame = sqlContext.read.parquet("hdfs://192.168.2.31:8020/user/hive/warehouse/pubg.db/aggregate_p")
  }

  def test9(sc: SparkContext): Unit ={
  }
}
