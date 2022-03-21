package com.kh.withgraphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object PlusCourtCheminSansPregel {

//  def parsLine(line:String)={
//
//    val fields = line.split(",")
//
//    var adjescenceList = Array[Long]
//    adjescenceList(0) = fields(0)
//    var test= (1, (3,4))
//    var i = 1
//    while(i < fields.length) {
//
//    }
//
//    val q1 = fields(0).toLong
//    val q2 = fields(2).toLong
//    val alpah = fields(1).toString
//
//    Edge(q1,q2,alpah)
//  }

  def main(args: Array[String]): Unit =  {

    Logger.getLogger("org").setLevel((Level.ERROR))
    val sc = new SparkContext("local[*]", "PlusCourtChemin_with_pregel")

    var graphe:RDD[(Int, (ListBuffer[(Int, Double)], Double))] = null

    graphe = sc.parallelize(Seq(
      (1, (ListBuffer((2, 2.0), (3, 4.0)),0.0)),
      (2, (ListBuffer((3, 1.0), (5, 7.0)), Double.PositiveInfinity)),
      (3, (ListBuffer((4, 3.0), (5, 1.0)), Double.PositiveInfinity)),
      (4, (ListBuffer((6, 1.0)), Double.PositiveInfinity)),
      (5, (ListBuffer((4, 2.0), (6, 5.0)), Double.PositiveInfinity)),
      (6, (ListBuffer(), Double.PositiveInfinity))
    ))

    for(i <- 1 to 10){

      val result = graphe.flatMap(value => {
        var list: ListBuffer[(Int, Double)] = ListBuffer[(Int, Double)]()

        for (x <- (value._2)._1){

          list.append((x._1, x._2 + (value._2)._2))
        }

        list
      }).reduceByKey((x, y) => math.min(x ,y)).fullOuterJoin(graphe)
        .map{ case (id, (left, right)) => {
          var value = 0.0
          if(left == None)  value = right.get._2
          else  value = left.get

          (id, (right.get._1, value))

        } }
      graphe = result
    }

    val result = graphe.map{
      case (id, (_, value)) => (id, value)
    }

    result.foreach(println)


  }



}
