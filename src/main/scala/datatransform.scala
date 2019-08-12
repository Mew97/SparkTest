import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON

import scala.collection.parallel.ForkJoinTaskSupport
import scala.io.Source


object datatransform {
  def main(args: Array[String]): Unit = {
    data(args(0).toInt)
  }
  def data(num: Int): Unit ={
    val writer = new PrintWriter(new File("/home/data/cardata1.csv"))
    val files = new File("/home/data/data").listFiles().par
    files.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(num))
    for (f <- files){
      val src = Source.fromFile(f)
      src.getLines().map(x=>JSON.parseObject(x)).foreach(x=>{
        val arr = x.getJSONArray("startTravel")
        for (i <- 0 until arr.size()) {
          val nObject = arr.getJSONObject(i)
          val number = x.getString("number")
          val type0 = x.getString("type")
          val color = x.getString("color")
          val nodeName = nObject.getString("nodeName")
          val drivingDirection = nObject.getString("drivingDirection")
          val longitude = nObject.getString("longitude")
          val latitude = nObject.getString("latitude")
          val afterMoment = nObject.getString("afterMoment")
          val date0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(afterMoment)
          val date = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format(date0)
          writer.write(s"$number/$date/$nodeName,$number,$type0,$color,$nodeName,$drivingDirection,$longitude,$latitude,$date\n")
        }
      })
      src.close()
    }
    writer.close()
  }

  def test(): Unit ={
    var a = 1
    val files = new File("/Users/zhubo/Desktop/未命名文件夹 2").listFiles().par
    files.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(4))
    for (f <- files){
      val src = Source.fromFile(f)
      src.getLines().map(x=>JSON.parseObject(x)).foreach(x=>{
        val arr = x.getJSONArray("startTravel")
        for (i <- 0 until arr.size()) {
          val nObject = arr.getJSONObject(i)
          val number = x.getString("number")
          val type0 = x.getString("type")
          val color = x.getString("color")
          val nodeName = nObject.getString("nodeName")
          val drivingDirection = nObject.getString("drivingDirection")
          val longitude = nObject.getString("longitude")
          val latitude = nObject.getString("latitude")
          val afterMoment = nObject.getString("afterMoment")
          val date0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(afterMoment)
          val date = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format(date0)
          println(s"$number/$date/$nodeName,$number,$type0,$color,$nodeName,$drivingDirection,$longitude,$latitude,$date")
          //println(number+"/"+afterMoment+"/"+nodeName+","+number+","+type0+","+color+","+nodeName+","+drivingDirection+","+longitude+","+latitude+","+afterMoment+"\n")
        }
      })
      src.close()
    }

  }
}
