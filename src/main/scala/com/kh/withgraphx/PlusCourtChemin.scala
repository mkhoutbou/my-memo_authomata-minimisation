package com.kh.withgraphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators

object PlusCourtChemin {

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel((Level.ERROR))
    val sc = new SparkContext("local[*]","PlusCourtChemin_with_pregel")
    // A graph with edge attributes containing distances
    val graphRDD: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 2 //sommet source
    // initialiser les sommets, excepte le sommet source, a l'infini.
    val resultGraphRDD: Graph[Double, Double] = graphRDD.mapVertices(
      (id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity
    )
    .pregel(Double.PositiveInfinity)( // message initial
      (id, dist, newDist) => math.min(dist, newDist), // Programme de sommet (Vprog)
      triplet => {  // Programme d'envoie message (sendMsg)
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // programme de merge (mergeMsg)
    )

//    println(sssp.vertices.collect.mkString("\n"))
    println("=========================================*******************************====================")
    println(graphRDD.edges.collect.mkString("\n"))
  }

}
