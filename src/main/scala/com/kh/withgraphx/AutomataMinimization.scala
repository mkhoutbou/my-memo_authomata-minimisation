package com.kh.withgraphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object AutomataMinimization {
  def parseEdgeLine(line: String) = {

    val fields: Array[String] = line.split(",")
    val q1 = fields(0).toLong
    val q2 = fields(2).toLong
    val alpha = fields(1)
    Edge(q1, q2, alpha)

  }


  def vprog(vertexId: VertexId, vertexData: String, message: String): String = {

    if (message.contains("stop")) {
      val nodeClass = ":".r.split(vertexData)(2)
      println(s"my ID = ${vertexId} I received stop : ${nodeClass}")
      return nodeClass
    }

    if (vertexId == -1) {

      if (!message.isEmpty && !message.contains("1") && !vertexData.equals("alt")) return "alt"
      return ""
    }
    else {
      if (message.isEmpty) return vertexData
      val arrayMessages = message.split("_").sortWith(_.compareTo(_) < 0)
        .map(msg => msg.split("-").last)
      val currentSize: Int = arrayMessages.toSet.size
      val previousDatas = vertexData.split(":")
      val previousSize: Int = previousDatas.head.toInt
      val previousClass: String = previousDatas.last
      var status = ""
      if (currentSize == previousSize) status = "0"
      else status = "1"
      val currentData: String = currentSize + ":" + status + ":" + previousClass + arrayMessages.reduceLeft(_ + _)
      return currentData

    }
  }

  def sendMsg(triplet: EdgeTriplet[String, String]): Iterator[(VertexId, String)] = {

    if (triplet.dstAttr.length == 1) return Iterator((triplet.srcId, triplet.attr + "-" + triplet.dstAttr))
    //    if(triplet.dstAttr.equals("stop")) return Iterator()

    if (triplet.dstId == -1) { // Du Superviseur (noeud special) vers les autres noeuds

      if (triplet.dstAttr.equals("alt")) return Iterator((triplet.srcId, "stop"))

      return Iterator.empty

    }
    val vertexDatas = triplet.dstAttr.split(":")
    if (vertexDatas.length == 1) return Iterator.empty
    val message = triplet.attr + "-" + vertexDatas.last
    if (triplet.srcId == -1) { // Des autres noeud vers le Superviseur ( noeud special )
      val status = vertexDatas(1)
      println(s"in sendMsg ==> status : ${status}")
      return Iterator((triplet.srcId, status))
    }
    // Entre les noeuds ( pas le Superviseur )
    return Iterator((triplet.srcId, message))

  }

  def reduceMsg(message1: String, message2: String): String = {
    message1 + "_" + message2
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("spark://spark:7077", "AutomataMinimization")

    val edgeLines = sc.textFile("edges.txt")
    val vertices = (sc.textFile("vertices.txt")
      .map(line => line.split(",")).map(part => (part(0).toLong, part(1))))
    val edges = edgeLines.map(parseEdgeLine)

    val graph = Graph(vertices, edges)
    val gra2: Graph[String, String] = graph.pregel[String]("", 100, EdgeDirection.In)(vprog, sendMsg, reduceMsg)
    gra2.vertices.collect.foreach(println)

  }

}
