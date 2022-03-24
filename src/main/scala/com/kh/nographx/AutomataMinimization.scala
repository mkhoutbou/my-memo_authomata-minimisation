package com.kh.nographx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AutomataMinimization {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel((Level.ERROR))
    val sc = new SparkContext("local[*]", "PlusCourtChemin_with_pregel")

    val alt = sc.longAccumulator("Alt")

    var edges = sc.parallelize(Seq(
      (0, "a", 0),
      (0, "b", 3),
      (3, "a", 3),
      (3, "b", 1),
      (1, "a", 1),
      (1, "b", 2),
      (2, "a", 2),
      (2, "b", 3)
    ))

    var vertexClass = sc.parallelize(Seq(
      (0, "1:0"),
      (1, "1:0"),
      (2, "1:0"),
      (3, "1:1")
    ))

    alt.add(1L)
    while (alt.value > 0) {
      alt.reset()
      var result1 = edges.map{ case (src, alpha, dest) => (dest, (alpha, src)) }
      var result2 = vertexClass.join(result1).map{case (_, (destClass, (alpha, src))) => (src, (alpha, destClass))}
      var result3 = result2.reduceByKey{
        case ( (alpha1, class1), (alpha2, class2) ) => {
          val value1 = if (alpha1.isEmpty) class1 else alpha1 +"-"+ class1
          val value2 = if (alpha2.isEmpty) class2 else alpha2 +"-"+ class2
          println(s"==============> ${value1 +"_"+ value2}")
          ("", value1 +"_"+ value2)
        }
      }.map{case (vertexId, (_, classes)) => (vertexId, classes)}.join(vertexClass)
      result3.foreach{
        case (id, (newClass,previousClass)) => {
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
      var result4 = result3.map{
        case (vertexId, (newClass, previousClass)) => {

          val newClassBlocks = "_".r.split(newClass).sortWith(_.compareTo(_) < 0)
            .map(alpha_class=> alpha_class.split("-").last.split(":").last)
          val currentClass = previousClass.split(":").last + newClassBlocks.reduceLeft(_+_)
          val currentBlocksSize = newClassBlocks.toSet.size
          val vertexClass = currentBlocksSize.toString +":"+ currentClass

          (vertexId, vertexClass)
        }
      }

      vertexClass = result4


    }

    vertexClass.foreach(println)


  }


}
