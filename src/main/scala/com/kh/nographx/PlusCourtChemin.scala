  package com.kh.nographx

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd.RDD

  import scala.collection.mutable.ListBuffer

  object PlusCourtChemin {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel((Level.ERROR))
      val sc = new SparkContext("spark://spark:7077", "PlusCourtChemin")

      val alt = sc.longAccumulator("Alt")

      var graphe: RDD[(Int, (ListBuffer[(Int, Double)], Double))] = null

      graphe = sc.parallelize(Seq(
        (1, (  ListBuffer((2, 2.0), (3, 4.0)),   0.0                      )  ),
        (2, (  ListBuffer((3, 1.0), (5, 7.0)),   Double.PositiveInfinity  )  ),
        (3, (  ListBuffer((4, 3.0), (5, 1.0)),   Double.PositiveInfinity  )  ),
        (4, (  ListBuffer((6, 1.0)),             Double.PositiveInfinity  )  ),
        (5, (  ListBuffer((4, 2.0), (6, 5.0)),   Double.PositiveInfinity  )  ),
        (6, (  ListBuffer(),                     Double.PositiveInfinity  )  )
      ))

      alt.add(1L)

      while (alt.value > 0) {
        alt.reset()
        val result1 = graphe.flatMap {
          case (srcId, (listDestIds, distFromOriginToSrc)) => {
            listDestIds.map{case (distId, distFromSrcToDest) => (distId, distFromSrcToDest + distFromOriginToSrc)}
          }
        }.reduceByKey((x, y) => math.min(x, y)).fullOuterJoin(graphe)

        result1.foreach { case (id, (newDistFromOrigin, right)) => {
            val previousDistFromOrigin = right.get._2
            var _newDistFromOrigin = 0.0
            if (newDistFromOrigin.isEmpty) _newDistFromOrigin = previousDistFromOrigin
            else _newDistFromOrigin = newDistFromOrigin.get
            if (_newDistFromOrigin < previousDistFromOrigin) alt.add(1L)
          }
        }
        val result2 = result1.map { case (id, (newDistFromOrigin, right)) => {
          val previousDistFromOrigin = right.get._2
          var _newDistFromOrigin = 0.0
          if (newDistFromOrigin.isEmpty) _newDistFromOrigin = previousDistFromOrigin
          else {
            if (previousDistFromOrigin < newDistFromOrigin.get) _newDistFromOrigin = previousDistFromOrigin
            else _newDistFromOrigin = newDistFromOrigin.get
          }
          (id, (right.get._1, _newDistFromOrigin))

        }
        }
        graphe = result2
      }

      val shortestPaths = graphe.map {
        case (id, (_, value)) => (id, value)
      }

      shortestPaths.foreach(println)


    }


  }
