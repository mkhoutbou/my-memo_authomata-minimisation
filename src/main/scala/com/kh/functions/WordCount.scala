package com.kh.functions

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {

"""----------------------------------------------------------------------------------------------------------------"""  
    """ Partie Configuration """
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","WordCount")
"""----------------------------------------------------------------------------------------------------------------"""    
    val text = sc.textFile(fileName)
    val word_counts = text
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x,y) => x + y)
"""----------------------------------------------------------------------------------------------------------------""" 
    """ Affichage """
    word_counts
      .collect()
      .foreach( (word_count) => println(s"${word_count._1} : ${word_count._2}"))
"""----------------------------------------------------------------------------------------------------------------"""   

  }
}
