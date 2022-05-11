package com.kh.functions

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {

"""----------------------------------------------------------------------------------------------------------------"""  
    """ Partie Configuration """
    val sc = new SparkContext("local[*]","WordCount")
"""----------------------------------------------------------------------------------------------------------------"""
    val fileName:String                 = "./UGB_university_article.txt" // nom du fichier
    val text:RDD[String]                = sc.textFile(fileName) // creation de RDD en chargeant un fichier
    val word_counts:RDD[(String, Int)]  = text.flatMap(line => line.split(" ")) // decouper en mot
                                             .map(word => (word, 1)) // creation des couple <cle, valeur> avec Map
                                             .reduceByKey((x,y) => x + y) // Cacul des occurence en utilisant ReduceByKey
"""----------------------------------------------------------------------------------------------------------------""" 
    """ Affichage """
    word_counts
      .collect() // repatrier les partitions au Driver ( master )
      .foreach{ case (word, count) => println(s"${word} : ${count}")} // afficher
"""----------------------------------------------------------------------------------------------------------------"""   

  }
}
