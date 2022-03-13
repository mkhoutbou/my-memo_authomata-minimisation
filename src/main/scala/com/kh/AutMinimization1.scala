package com.kh

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

object AutMinimization1 {


  def parseEdgeLine(line: String) = {

    val fields: Array[String] = line.split(",")

    val q1 = fields(0).toLong
    val q2 = fields(2).toLong
    val alpha = fields(1)

    Edge(q1, q2, alpha)
  }


  def vprog(vertexId: VertexId, vertexData: String, message: String): String = {
    if(message.isEmpty) return vertexData
    val newData = vertexData + "_".r.split(message).sortWith(_.compareTo(_) < 0)
      .map(msg=> "-".r.split(msg).last).reduceLeft(_ + _)
    newData
  }

  def sendMsg(triplet: EdgeTriplet[String, String]): Iterator[(VertexId, String)] = {
    val message = triplet.attr + "-" + triplet.dstAttr
    Iterator((triplet.srcId, message))
  }

  def reduceMsg(message1: String, message2: String): String = {
    message1 + "_" + message2
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendByAge")

    val edgeLines = sc.textFile("edges.txt")
    val vertices = (sc.textFile("vertices.txt")
      .map(line => line.split(",")).map(part => (part(0).toLong, part(1))))
    val edges = edgeLines.map(parseEdgeLine)

    val graph = Graph(vertices, edges)
    val gra2: Graph[String,String] = graph.pregel[String]("",3,EdgeDirection.In)(vprog,sendMsg, reduceMsg)
    gra2.edges.saveAsTextFile("./edgesOut")
    gra2.vertices.saveAsTextFile("./verOut")

  }

}
