package com.kh.functions

import org.apache.spark.SparkContext

object MeanWordLength {

  def main(args: Array[String]) {
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","WordCount")
    val text = sc.textFile(fileName)
    val words = text.flatMap(line => " ".r.split(line))
    val word_key_value = words.map(word => (word.length, 1))
    val word_counts = word_key_value.reduceByKey((x,y) => x + y)
    val numerator_denominator = word_counts.reduce((first, second) => {

      val numerateur1 = if(first._1 > 0) first._1 * first._2 else -1 * first._1
      val numerateur2 = if(second._1 > 0) second._1 * second._2 else -1 * second._1
      val numerator = -1 * (numerateur1 + numerateur2 )
      val denominator = first._2 + second._2
      (numerator, denominator)

    })
    println(s"**************************** ============== *****************************")
    println(s"Numerateur : ${numerator_denominator._1} ------------ Denominateur :${numerator_denominator._2}")
  }
}
