package com.kh.functions

import org.apache.spark.SparkContext

object Anagramme {

  def main(args: Array[String]) {
    val fileName = "./words_test.txt" // nom du fichier
    val sc = new SparkContext("local[*]","WordCount")
    val text = sc.textFile(fileName)
    val words = text.flatMap(line => " ".r.split(line))
    val word_key_value = words.map(word => (word.sorted, null))
    val distinct_word_count = word_key_value.reduceByKey((x,y) => y).collect()

    println(s"**************************** ============== *****************************")
    println(s"Nombre ===================> : ${distinct_word_count.length}")
  }

}
