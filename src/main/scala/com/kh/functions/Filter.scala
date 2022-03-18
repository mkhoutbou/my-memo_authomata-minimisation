package com.kh.functions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Filter {

  def main(args: Array[String]) {
    val logFile = "./words_test.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a"))
    val numBs = logData.filter(line => line.contains("B"))
    println(s"Lines with a: ${numAs}, Lines with b: ${numBs}")
    numAs.collect().foreach(println)
    numBs.collect().foreach(println)
    spark.stop()
  }

}
