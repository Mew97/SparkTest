import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object TaoBao {

  def main(args: Array[String]): Unit = {
    test2()

  }

  def test1(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val spark = new SparkContext(conf)
    val sqlContext = new HiveContext(spark)
    val df = sqlContext.read.parquet("hdfs://master:8020/user/hive/warehouse/taobao.db/userb")
    sqlContext.setConf("spark.sql.dialect","hiveql")
    import org.apache.spark.sql.functions._

    df.filter(df("behavior")==="pv")
      .groupBy("shop_id")
      .count()
      .orderBy(desc("count"))
      .limit(3)
      .show()
  }

  def test2(): Unit ={
    val conf = new SparkConf().setAppName("mySpark")
    val spark = new SparkContext(conf)
    val sqlContext = new HiveContext(spark)
    sqlContext.setConf("spark.sql.dialect","hiveql")
    sqlContext.read.parquet("hdfs://master:8020/user/hive/warehouse/taobao.db/userb")
      .registerTempTable("userB")
    val df = sqlContext.sql("select shop_id, count(*) count from userB group by shop_id order by count desc limit 3")
      .cache()
    df.show()
  }
}
