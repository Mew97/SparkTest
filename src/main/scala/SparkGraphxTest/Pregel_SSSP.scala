package SparkGraphxTest

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source


object Pregel_SSSP {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Pregel_SSSP").setMaster("local[8]")
    val sc = new SparkContext(conf)
    // A graph with edge attributes containing distances
//    val graph: Graph[Long, Double] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)

    val edges = sc.textFile("E:\\py_program\\ScrapyTest\\relationship.csv")
      .map(_.split(",")).map(x=>Edge(x(0).toLong,x(1).toLong,1.toDouble))
    val graph: Graph[Long, Double] = Graph.fromEdges(edges, 1.toLong)

    //graph.edges.foreach(println)

    val sourceId: VertexId = 1104 // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph : Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[VertexId](sourceId))
      else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[VertexId]()), 25, EdgeDirection.Out)(

      // Vertex Program
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      // Send Message
      triplet => {
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },

      //Merge Message
      (a, b) => if (a._1 < b._1) a else b)

    //println(sssp.vertices.collect.mkString("\n"))
    val rs = sssp.vertices.filter(x=>x._1==1202).first()._2._2

    val src = Source.fromFile("E:\\py_program\\ScrapyTest\\node.csv")
    val bus_l = ListBuffer[(String,String)]()
    src.getLines().map(_.split(",")).foreach(x=> bus_l+= Tuple2(x(0),x(1)) )
    val bus_m = bus_l.toMap
    src.close()

    rs.foreach(x=>println(bus_m.getOrElse(x.toString,0)))
  }
}
