package com.kh.withgraphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object AutomataMinimizationPregel {
val alphabetSize:Int = 2
def parseEdgeLine(line: String) = {

  val fields: Array[String] = line.split(",")
  val q1 = fields(0).toLong
  val q2 = fields(2).toLong
  val alpha = fields(1)
  Edge(q1, q2, alpha)

}


def vProg(vertexId: VertexId, vertexData: String, message: String): String = {


  if (vertexId == -1) { // si c'est le noeud special

    if (!message.isEmpty && !message.contains("1") && !vertexData.equals("alt")) return "alt"
    return ""
  }
  else { // si c'est un autre noeud ( autre que le noeud especial )

    // si le message c'est le message d'initialisation
    if (message.isEmpty) return vertexData

    // s'il recoit un signal d'arret du noeud especial
    if (message.contains("stop")) {
      var nodeClass = vertexData.split(":")(2)
      var classLength = nodeClass.length
      classLength = classLength/(alphabetSize + 1)
      nodeClass = nodeClass.substring(0, classLength)
      return nodeClass
    }

    // calcul de la nouvelle classe
    val arrayMessages = message.split("_").sortWith(_.compareTo(_) < 0)
      .map(msg => msg.split("-").last)
    val currentSize: Int = arrayMessages.toSet.size
    val previousData = vertexData.split(":")
    val previousSize: Int = previousData.head.toInt
    val previousClass: String = previousData.last
    var status = ""
    if (currentSize == previousSize) status = "0"
    else status = "1"
    val currentData: String = currentSize + ":" + status + ":" + previousClass + arrayMessages.reduceLeft(_ + _)
    return currentData

  }
}

def sendMsg(triplet: EdgeTriplet[String, String]): Iterator[(VertexId, String)] = {

  // Du Superviseur (noeud special) vers les autres noeuds
  if (triplet.dstId == -1) {

    if (triplet.dstAttr.equals("alt")) return Iterator((triplet.srcId, "stop"))

    return Iterator.empty

  }
  // si c'est le message envoye par les autres noeuds (pas le Superviseur)
  if (triplet.dstAttr.length == 1) return Iterator((triplet.srcId, triplet.attr + "-" + triplet.dstAttr))

  val vertexData = triplet.dstAttr.split(":")
  if (vertexData.length == 1) return Iterator.empty
  val message = triplet.attr + "-" + vertexData.last

  // Des autres noeud vers le Superviseur ( noeud special )
  if (triplet.srcId == -1) {
    val status = vertexData(1)
    println(s"in sendMsg ==> status : ${status}")
    return Iterator((triplet.srcId, status))
  }

  // Entre les noeuds ( sans le Superviseur )
  return Iterator((triplet.srcId, message))

}

def reduceMsg(message1: String, message2: String): String = {
  message1 + "_" + message2
}

def main(args: Array[String]): Unit = {

"""----------------------------------------------------------------------------------------------------------------"""
            """ La Partie Configuration """
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "AutomataMinimizationPregel")

"""----------------------------------------------------------------------------------------------------------------"""
            """ Le Programme """

  val vertices = sc.textFile("vertices.txt")
    .map(line => line.split(","))
    .map(part => (part(0).toLong, part(1)))

  val edges = sc.textFile("edges.txt")
    .map(parseEdgeLine)

  val graph = Graph(vertices, edges)
  val gra2: Graph[String, String] = graph.pregel[String]("", 100, EdgeDirection.In)(vProg, sendMsg, reduceMsg)

"""----------------------------------------------------------------------------------------------------------------"""
              """ L'Affichage """
  gra2.vertices.collect.foreach(println)

}

}
