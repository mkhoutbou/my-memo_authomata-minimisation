package com.kh
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

object AutMinimization0 {

  def parsLine(line:String)={

    val fields = line.split(",")

    val q1 = fields(0).toLong
    val q2 = fields(2).toLong
    val alpah = fields(1)

    Edge(q1,q2,alpah)
  }

  def vprog(vertexId: VertexId,x:String, y:String): String = {
    println(vertexId.toLong+ " : " + y)
    vertexId.toString + y
  }

  def sendMsg(triplet: EdgeTriplet[String,String]): Iterator[(VertexId, String)] ={
    val attr =  "_(" + triplet.srcId + "_" + triplet.attr + ")_"
    Iterator((triplet.srcId,triplet.dstAttr))
  }

  def reduceMsg(msg1: String, msg2: String):String = {
    msg1 + msg2
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","FriendByAge")

    val edgeLines = sc.textFile("edges.txt")
    val finales = sc.textFile("finales.txt").map(finales => finales.toLong)

    val edges = edgeLines.map(parsLine)
    val graph = Graph.fromEdges(edges,"..").cache()

    val gra2: Graph[String,String] = graph.pregel[String]("_",5,EdgeDirection.In)(vprog,sendMsg, reduceMsg)
    gra2.edges.saveAsTextFile("./edgesOut")
    gra2.vertices.saveAsTextFile("./verOut")

  }

}
