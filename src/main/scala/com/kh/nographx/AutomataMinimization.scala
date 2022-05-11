  package com.kh.nographx

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.sparkproject.jetty.server.RequestLog.Collection

  object AutomataMinimization {

    class BlockId(blockId: String, size: Int)
    def main(args: Array[String]): Unit = {

"""----------------------------------------------------------------------------------------------------------------"""
""" La Partie Configuration """
Logger.getLogger("org").setLevel((Level.ERROR))
val sc = new SparkContext("spark://spark:7077", "AutomataMinimizationPregel")

"""----------------------------------------------------------------------------------------------------------------"""
      """ Le Programme """

      // est une variable partagee qui permet au master de savoir si une nouvelle classe a ete cree par un worker
      val voteAlt = sc.longAccumulator("Alt")

      var transitionsRDD = sc.parallelize(Seq(
        (0, "a", 0),
        (0, "b", 3),
        (3, "a", 3),
        (3, "b", 1),
        (1, "a", 1),
        (1, "b", 2),
        (2, "a", 2),
        (2, "b", 3)
      ))

      var stateBlockIdsRDD = sc.parallelize(Seq(
        (0, ("0", 1)),
        (1, ("0", 1)),
        (2, ("0", 1)),
        (3, ("1", 1))
      ))

      var result1 = transitionsRDD.map{ case (src, alpha, dest) => (dest, (alpha, src)) }

      voteAlt.add(1L)
      while (voteAlt.value > 0) {

        voteAlt.reset()

        // Calcul des classes
        var result2 = stateBlockIdsRDD
          .join(result1)
          .map{case (_, (destBlockId, (alpha, src))) => (src, (alpha, destBlockId))}
          .groupByKey()
          .join(stateBlockIdsRDD)

        // Mis a jour des classes
        stateBlockIdsRDD = result2
          .map{ case (stateId, (destBlockIdsCollection, statePreviousBlockId)) =>

            val blockId = statePreviousBlockId._1 + destBlockIdsCollection.toSeq.sortWith{
              case ((alpha1,_), (alpha2,_)) => alpha1.compareTo(alpha2) < 0
            }.map{case (_, destBlockId) => destBlockId._1}.reduceLeft(_+_)
            var blockIdsSet = destBlockIdsCollection.map{case (_, (blockId, _)) => blockId}.toSet
            blockIdsSet += statePreviousBlockId._1
            val newBlockIdSize = blockIdsSet.size
            val vertexClass= (blockId, newBlockIdSize)

            (stateId, vertexClass)
          }

        // detection de creation de nouvelles classes
        result2.foreach{case (_, (destBlockIdsCollection,statePreviousBlockId)) =>
            var blockIdsSet = destBlockIdsCollection.map{case (_, (blockId, _)) => blockId}.toSet
            blockIdsSet += statePreviousBlockId._1
            val newBlockIdSize = blockIdsSet.size
            if(statePreviousBlockId._2 < newBlockIdSize ) {
              voteAlt.add(1L)
            }
        }

      }
"""----------------------------------------------------------------------------------------------------------------"""
      """ L'Affichage """
      stateBlockIdsRDD.collect.foreach(println)
    }
"""----------------------------------------------------------------------------------------------------------------"""
  }
