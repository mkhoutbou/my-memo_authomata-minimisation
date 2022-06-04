  package com.kh.nographx

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd.RDD
  import org.sparkproject.jetty.server.RequestLog.Collection

  object AutomataMinimization {

    case class BlockId(value: String, size: Int)


    def main(args: Array[String]): Unit = {

"""----------------------------------------------------------------------------------------------------------------"""
      """ La Partie Configuration """
      Logger.getLogger("org").setLevel((Level.ERROR))
      val sc = new SparkContext("spark://spark:7077", "AutomataMinimization")

"""----------------------------------------------------------------------------------------------------------------"""
      """ Le Programme """

      // est une variable partagee qui permet au master de savoir si une nouvelle classe a ete cree par un worker
      val VoteContinue = sc.longAccumulator("Alt")

      val transitionsRDD: RDD[(Long, String, Long)] = sc.makeRDD(Seq(
        (0, "a", 0),
        (0, "b", 3),
        (3, "a", 3),
        (3, "b", 1),
        (1, "a", 1),
        (1, "b", 2),
        (2, "a", 2),
        (2, "b", 3)
      ))

      var stateBlockIdsRDD: RDD[(Long, BlockId)] = sc.makeRDD(Seq(
        (0, BlockId("0", 1)),
        (1, BlockId("0", 1)),
        (2, BlockId("0", 1)),
        (3, BlockId("1", 1))
      ))

      var result0 = transitionsRDD.map{ case (src, alpha, dest) => (dest, (alpha, src)) }

      VoteContinue.add(1L)
      while (VoteContinue.value > 0) {

        VoteContinue.reset()

        var result1 = stateBlockIdsRDD
          .join(result0)
          .map{case (_, (destBlockId, (alpha, src))) => (src, (alpha, destBlockId))}
          .groupByKey()
          .join(stateBlockIdsRDD)

        // Mis a jour des
        stateBlockIdsRDD = result1
          .map{ case (stateId, (destBlockIdsCollection, statePreviousBlockId)) =>

            val blockId = statePreviousBlockId.value + destBlockIdsCollection.toSeq.sortWith{
              case ((alpha1,_), (alpha2,_)) => alpha1.compareTo(alpha2) < 0
            }.map{case (_, destBlockId) => destBlockId.value}.reduceLeft(_+_)
            var blockIdsSet = destBlockIdsCollection.map{case (_, blockId) => blockId.value}.toSet
            blockIdsSet += statePreviousBlockId.value
            val newBlockIdSize = blockIdsSet.size
            val vertexClass= BlockId(blockId, newBlockIdSize)

            (stateId, vertexClass)
          }

        // detection de creation de nouvelles classes
        result1.foreach{case (_, (destBlockIdsCollection,statePreviousBlockId)) =>
            var blockIdsSet = destBlockIdsCollection.map{case (_, blockId) => blockId.value}.toSet
            blockIdsSet += statePreviousBlockId.value
            val newBlockIdSize = blockIdsSet.size
            if(statePreviousBlockId.size < newBlockIdSize ) {
              VoteContinue.add(1L)
            }
        }

      }
"""----------------------------------------------------------------------------------------------------------------"""
      """ L'Affichage """
      stateBlockIdsRDD.collect.foreach(println)
    }
"""----------------------------------------------------------------------------------------------------------------"""
  }