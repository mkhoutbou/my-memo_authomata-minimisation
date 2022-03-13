package com.kh

import org.apache.spark
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.storage.StorageLevel
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object TestGraphx {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContext.getOrCreate()
    // Create an RDD for the vertices
    val st: StorageLevel = StorageLevel.MEMORY_ONLY
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Seq(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + "(" +triplet.srcAttr._2+")"+ " is the " + triplet.attr + " of " + triplet.dstAttr._1+ "(" +triplet.dstAttr._2+")")
    facts.foreach(println(_))
  }

}
