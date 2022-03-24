package com.kh.withgraphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object AutomataMinimization {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel((Level.ERROR))
    val sc = new SparkContext("local[*]", "PlusCourtChemin_with_pregel")

    val alt = sc.longAccumulator("Alt")

//    var graphe: RDD[(Int, (ListBuffer[(Int, Double)], Double))] = null

    var graph = sc.parallelize(Seq(
      (0, "a", 0),
      (0, "b", 3),
      (3, "a", 3),
      (3, "b", 1),
      (1, "a", 1),
      (1, "b", 2),
      (2, "a", 2),
      (2, "b", 3)
    ))

    val alphabetSize = 2
    var result1 = graph.map{
      case (src, alpha, dest) => {
        var srcClass = ""
        var destClass = ""
        (src, (alpha, "1:0"))
      }
    }
    var riesult1 = graph.map{
      case (src, alpha, dest) => {
        val srcClass = ""
        val destClass = ""
        if(dest == 3) ( src,(alpha, "1:1") )
        else (src, (alpha, "1:0"))
      }
    }

    result1.foreach(println)
    println("********************************************")

    alt.add(1L)
    while (alt.value > 0) {
      alt.reset()
      var result2 = result1.reduceByKey{
        case ( (alpha1, class1), (alpha2, class2) ) => {
          val value1 = if (alpha1.isEmpty) class1 else alpha1 +"-"+ class1
          val value2 = if (alpha2.isEmpty) class2 else alpha2 +"-"+ class2
          println(s"==============> ${value1 +"_"+ value2}")
          ("", value1 +"_"+ value2)
        }
      }.join(result1)

      result2.foreach{
        case (id, ((_, newClass), (_,previousClass))) => {
            println(
              s"""--------------------------------------------
                |VertexId : $id
                |New Class : $newClass
                |previous Class : $previousClass
                |""".stripMargin)
            val newClassSize = "_".r.split(newClass).map(x => "-".r.split(x)(1)).toSet.size
            if(previousClass.length == 1) {
              if(newClassSize != 1) alt.add(1L)
            }else {
              val previousClassSize = ":".r.split(previousClass)(0).toInt
              if(previousClassSize != newClassSize) alt.add(1L)
            }
        }
      }

      val result3 = result2.map{
        case (vertexId, ((_, newClass), (alpha, previousClass))) => {

          val newClassBlocks = "_".r.split(newClass).sortWith(_.compareTo(_) < 0)
            .map(alpha_class=> alpha_class.split("-").last.split(":").last)
          val currentBlocksSize = newClassBlocks.toSet.size
          val vertexClass = currentBlocksSize.toString +":"+ newClassBlocks.reduceLeft(_+_)

          (vertexId, (alpha, vertexClass))
        }
      }

      result1 = result3


    }



    result1.foreach(println)


  }


}
