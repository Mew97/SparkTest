import scala.collection.mutable.ListBuffer
import scala.io.Source

object Test {
  def main(args: Array[String]): Unit = {
    var count = 0
    val a = ListBuffer[Any]()
    a ++= List('a',1,"a")
    println(a)

    val src = Source.fromFile("E:\\py_program\\ScrapyTest\\node.csv")
    val bus_l = ListBuffer[(String,String)]()
    src.getLines().map(_.split(",")).foreach(x=> bus_l+= Tuple2(x(0),x(1)) )
    println(bus_l.toMap)
    println(bus_l)
    src.close()
  }
}
