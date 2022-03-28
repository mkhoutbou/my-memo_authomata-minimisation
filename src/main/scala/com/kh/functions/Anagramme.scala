package com.kh.functions

import org.apache.spark.SparkContext

object Anagramme {

  def main(args: Array[String]) {

"""----------------------------------------------------------------------------------------------------------------"""
    """ La Partie Configuration """
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","WordCount")

"""----------------------------------------------------------------------------------------------------------------"""
    """ Le Programme """
    val distinct_word_count = sc.textFile(fileName)
      .flatMap(line => line.split(" "))
      .map(word => (word.sorted, null))
      .reduceByKey((x,y) => y).collect()
"""----------------------------------------------------------------------------------------------------------------"""
    """ L'Affichage """
    println(s"**************************** ============== *****************************")
    println(s"Nombre ===================> : ${distinct_word_count.length}")
  }
"""----------------------------------------------------------------------------------------------------------------"""

}
