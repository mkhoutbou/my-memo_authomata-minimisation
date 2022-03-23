package com.kh.nographx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object PlusCourtChemin {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel((Level.ERROR))
    val sc = new SparkContext("local[*]", "PlusCourtChemin_with_pregel")

    var alt = sc.longAccumulator("Alt")

    var graphe: RDD[(Int, (ListBuffer[(Int, Double)], Double))] = null

    graphe = sc.parallelize(Seq(
      (1, (ListBuffer((2, 2.0), (3, 4.0)), 0.0)),
      (2, (ListBuffer((3, 1.0), (5, 7.0)), Double.PositiveInfinity)),
      (3, (ListBuffer((4, 3.0), (5, 1.0)), Double.PositiveInfinity)),
      (4, (ListBuffer((6, 1.0)), Double.PositiveInfinity)),
      (5, (ListBuffer((4, 2.0), (6, 5.0)), Double.PositiveInfinity)),
      (6, (ListBuffer(), Double.PositiveInfinity))
    ))
    alt.add(1L)
    while (alt.value > 0) {
      alt.reset()
      val result1 = graphe.flatMap(
        value => {
          value._2._1.map(x => (x._1, x._2 + (value._2)._2))
        }
      ).reduceByKey((x, y) => math.min(x, y)).fullOuterJoin(graphe)

      result1.foreach {
        case (id, (left, right)) => {
          val previousValue = right.get._2
          var value = 0.0
          if (left == None) value = previousValue
          else value = left.get
          if (value < previousValue) alt.add(1L)
        }
      }
      val result2 = result1.map { case (id, (left, right)) => {
        val previousValue = right.get._2
        var value = 0.0
        if (left == None) value = previousValue
        else {
          if (previousValue < left.get) value = previousValue
          else value = left.get
        }
        (id, (right.get._1, value))

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
