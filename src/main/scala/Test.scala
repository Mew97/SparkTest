import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object Test {
  def main(args: Array[String]): Unit = {
//    var count = 0
//    val a = ListBuffer[Any]()
//    a ++= List('a',1,"a")
//    println(a)

    val host = "192.168.2.78"
    val port = 6379
    val timeout = 10000
    val jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout)
    val jedis = jedisPool.getResource
    val strings = jedis.randomKey()
    println(jedis.get("normal-car:冀ADIB33"))
    println(strings)
    jedis.close()

//    val src = Source.fromFile("E:\\py_program\\ScrapyTest\\node.csv")
//    val bus_l = ListBuffer[(String,String)]()
//    src.getLines().map(_.split(",")).foreach(x=> bus_l+= Tuple2(x(0),x(1)) )
//    println(bus_l.toMap)
//    println(bus_l)
//    src.close()
  }
}
