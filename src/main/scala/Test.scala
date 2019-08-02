import scala.collection.mutable.ListBuffer

object Test {
  def main(args: Array[String]): Unit = {
    var count = 0
    val a = ListBuffer[Any]()
    a ++= List('a',1,"a")
    println(a)
  }
}
