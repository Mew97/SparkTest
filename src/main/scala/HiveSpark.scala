import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object HiveSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mySpark")
    val spark = new SparkContext(conf)
    val hiveContext = new HiveContext(spark)
    hiveContext.setConf("spark.sql.dialect","hiveql")
    hiveContext.sql("create external table taobao.userBehavior2(user_id string,shop_id string,cate_id string,behavior string,time string)ROW FORMAT DELIMITED FIELDS TERMINATED BY ','location '/taobao/'")
      .show()
  }
}
