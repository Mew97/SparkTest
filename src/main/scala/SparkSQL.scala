import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    val sc = new SparkContext(conf)
    val distFile = sc.textFile("/Users/zhubo/Desktop/111.txt")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  }
}
