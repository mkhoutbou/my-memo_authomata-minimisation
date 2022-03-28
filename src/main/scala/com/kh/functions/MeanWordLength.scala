package com.kh.functions

import org.apache.spark.SparkContext

object MeanWordLength {

  def main(args: Array[String]) {
"""----------------------------------------------------------------------------------------------------------------"""
    """ La Partie Configuration """
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","MeanWordLength")

"""----------------------------------------------------------------------------------------------------------------"""
    """ Le Programme """

    val (numerator, denominator ) = sc.textFile(fileName)
      .flatMap(line => line.split(" "))
      .map(word => (word.length, 1))
      .reduceByKey((x,y) => x + y)
      .reduce{ case ((nume1, denomi1), (nume2, denomi2)) => {

        val numerator1 = if(nume1 > 0) nume1 * denomi1 else -1 * nume1
        val numerator2 = if(nume2 > 0) nume2 * denomi2 else -1 * nume2
        val numerator = -1 * (numerator1 + numerator2 )
        val denominator = denomi1 + denomi2
        (numerator, denominator)
      }}


"""----------------------------------------------------------------------------------------------------------------"""
    """ L'Affichage """
    println(s"**************************** ============== *****************************")
    println(s"Numerateur : ${-1*numerator} ------------ Denominateur :${denominator}")

"""----------------------------------------------------------------------------------------------------------------"""

  }
}
