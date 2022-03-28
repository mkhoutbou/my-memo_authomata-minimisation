  package com.kh.nographx

  import org.apache.log4j.{Level, Logger}
        import org.apache.spark.SparkContext
        import org.apache.spark.rdd.RDD

        import scala.collection.mutable.ListBuffer

  object PlusCourtChemin {

          def main(args: Array[String]): Unit = {

"""----------------------------------------------------------------------------------------------------------------"""
            """ La Partie Configuration """
            Logger.getLogger("org").setLevel((Level.ERROR))
            val sc = new SparkContext("spark://spark:7077", "PlusCourtChemin")

"""----------------------------------------------------------------------------------------------------------------"""
            """ Le Programme """

            // C'est une variable partagee qui permet au master si il doit sortir de la boucle while ou pas
            val voteAlt = sc.longAccumulator("Alt")

            val graphRDD = sc.parallelize(Seq(
              (1, 2.0, 2),
              (1, 4.0, 3),
              (2, 1.0, 3),
              (2, 7.0, 5),
              (3, 3.0, 4),
              (3, 1.0, 5),
              (4, 1.0, 6),
              (5, 2.0, 4),
              (5, 5.0, 6)
            ))
            var distFromOriginToVerticesRDD = sc.parallelize(Seq(
              (1, Double.PositiveInfinity),
              (2, Double.PositiveInfinity),
              (3, 0.0),
              (4, Double.PositiveInfinity),
              (5, Double.PositiveInfinity),
              (6, Double.PositiveInfinity),
            ))

            val result1 = graphRDD.map{
              case (srcId, distFromSrcToDest, destId) => {
                (srcId, (destId, distFromSrcToDest))
              }
            }
            voteAlt.add(1L)

            while(voteAlt.value > 0){

              voteAlt.reset()

              // calul de nouvel distance
              val result2 = result1
                .join(distFromOriginToVerticesRDD)
                .map{
                  case (srcId, ((destId, distFromSrcToDest), distFromOriginToSrc)) =>
                    (destId, distFromSrcToDest + distFromOriginToSrc)

                }
                .reduceByKey((x, y) => math.min(x, y))
                .fullOuterJoin(distFromOriginToVerticesRDD)

              // mis a jour des distances
              distFromOriginToVerticesRDD = result2.map { case (id, (newlyCalculatedDistFromOrigin, previousDistFromOrigin)) =>
                var newDistFromOrigin = 0.0
                if (newlyCalculatedDistFromOrigin.isEmpty) newDistFromOrigin = previousDistFromOrigin.get
                else {
                  if (previousDistFromOrigin.get < newlyCalculatedDistFromOrigin.get) newDistFromOrigin = previousDistFromOrigin.get
                  else newDistFromOrigin = newlyCalculatedDistFromOrigin.get
                }
                (id, newDistFromOrigin)
              }

              // verification de l'arret de la boucle
              result2.foreach { case (id, (newlyCalculatedDistFromOrigin, previousDistFromOrigin)) =>
                var newDistFromOrigin = 0.0
                if (newlyCalculatedDistFromOrigin.isEmpty) newDistFromOrigin = previousDistFromOrigin.get
                else newDistFromOrigin = newlyCalculatedDistFromOrigin.get
                if(newDistFromOrigin < previousDistFromOrigin.get) voteAlt.add(1L) // alors on continue
              }

            }
"""----------------------------------------------------------------------------------------------------------------"""
            """ L'Affichage """
            distFromOriginToVerticesRDD.collect.foreach(println)
          }
"""----------------------------------------------------------------------------------------------------------------"""

  }
