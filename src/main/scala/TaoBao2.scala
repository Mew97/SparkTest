import org.apache.spark.sql.SparkSession

object TaoBao2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("taobaotest")
      .config("spark.sql.dialect","hiveql")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.csv("hdfs://cdh1:8020/data/test")
    df.createOrReplaceTempView("test")
    spark.sql("select * from test")
      .show()
  }
}
