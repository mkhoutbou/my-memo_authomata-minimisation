package com.kh.functions

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","WordCount")
    val text = sc.textFile(fileName)
    val words = text.flatMap(line => " ".r.split(line))
    val word_key_value = words.map(word => (word, 1))
    val word_counts = word_key_value.reduceByKey((x,y) => x + y).collect()
    println(s"**************************** ============== *****************************")
    word_counts.foreach( (word_count) => println(s"${word_count._1} : ${word_count._2}"))
  }
}
