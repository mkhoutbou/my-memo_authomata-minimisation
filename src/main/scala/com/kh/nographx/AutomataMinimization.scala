  package com.kh.nographx

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext

  object AutomataMinimization {

    def main(args: Array[String]): Unit = {

"""----------------------------------------------------------------------------------------------------------------"""
      """ La Partie Configuration """
      Logger.getLogger("org").setLevel((Level.ERROR))
      val sc = new SparkContext("spark://spark:7077", "AutomataMinimizationPregel")

"""----------------------------------------------------------------------------------------------------------------"""
      """ Le Programme """

      // est une variable partagee qui permet au master de savoir si une nouvelle classe a ete cree par un worker
      val voteAlt = sc.longAccumulator("Alt")

      var edgesRDD = sc.parallelize(Seq(
        (0, "a", 0),
        (0, "b", 3),
        (3, "a", 3),
        (3, "b", 1),
        (1, "a", 1),
        (1, "b", 2),
        (2, "a", 2),
        (2, "b", 3)
      ))

      var vertexClassesRDD = sc.parallelize(Seq(
        (0, "1:0"),
        (1, "1:0"),
        (2, "1:0"),
        (3, "1:1")
      ))

      var result1 = edgesRDD.map{ case (src, alpha, dest) => (dest, (alpha, src)) }

      voteAlt.add(1L)
      while (voteAlt.value > 0) {

        voteAlt.reset()

        // Calcul des classes
        var result2 = vertexClassesRDD
          .join(result1)
          .map{case (_, (destClass, (alpha, src))) => (src, (alpha, destClass))}
          .reduceByKey{case ( (alpha1, class1), (alpha2, class2) ) =>
            val value1 = if (alpha1.isEmpty) class1 else alpha1 +"-"+ class1
            val value2 = if (alpha2.isEmpty) class2 else alpha2 +"-"+ class2
            ("", value1 +"_"+ value2)
          }
          .map{case (vertexId, (_, classes)) => (vertexId, classes)}
          .join(vertexClassesRDD)

        // Mis a jour des classes
        vertexClassesRDD = result2
          .map{ case (vertexId, (newClass, previousClass)) =>

            val newClassBlocks = newClass.split("_").sortWith(_.compareTo(_) < 0)
              .map(alpha_class=> alpha_class.split("-").last.split(":").last)
            val currentClass = previousClass.split(":").last + newClassBlocks.reduceLeft(_+_)
            val currentBlocksSize = newClassBlocks.toSet.size
            val vertexClass = currentBlocksSize.toString +":"+ currentClass

            (vertexId, vertexClass)
          }

        // detection de creation de nouvelles classes
        result2.foreach{case (id, (newClass,previousClass)) =>
            val newClassSize = newClass.split("_").map(x => "-".r.split(x)(1)).toSet.size
            if(previousClass.length == 1) {
              if(newClassSize != 1) voteAlt.add(1L)
            }else {
              val previousClassSize = previousClass.split(":")(0).toInt
              if(previousClassSize != newClassSize) voteAlt.add(1L)
            }
        }

      }
"""----------------------------------------------------------------------------------------------------------------"""
      """ L'Affichage """
      vertexClassesRDD.collect.foreach(println)
    }
"""----------------------------------------------------------------------------------------------------------------"""
  }
