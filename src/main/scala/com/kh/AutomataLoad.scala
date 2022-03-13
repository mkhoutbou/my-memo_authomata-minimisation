package com.kh


import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._
object AutomataLoad {

  def parsLine(line:String)={

    val fields = line.split(",")

    val q1 = fields(0).toLong
    val q2 = fields(2).toLong
    val alpah = fields(1).toString

    Edge(q1,q2,alpah)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel((Level.ERROR))

    val sc = new SparkContext("local[*]","FriendByAge")

    val edgeLines = sc.textFile("edges.txt")
    val finales = sc.textFile("finales.txt").map(finales => finales.toLong)

    val edges = edgeLines.map(parsLine)
    val graph = Graph.fromEdges(edges,"..").cache()
    graph.edges.collect().map(edge => println(s"(${edge.srcId})--[${edge.attr}]-->(${edge.dstId})"))

  }
}


