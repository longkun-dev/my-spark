package com.chenxii.myspark.sparkcore.ch3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    val lines = sparkContext.textFile("datas")
    val wordGroup: RDD[(String, Iterable[String])] = lines
      .flatMap(line => line.split(" "))
      .groupBy(word => word)
    val wordToCount = wordGroup.map {
      case (word, list) => (word, list.size)
    }
    wordToCount.foreach(println)

    println("=======")

    val wordGroup1: RDD[(String, Iterable[(String, Int)])] = lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupBy(wordItem => wordItem._1)
    val value = wordGroup1.map {
      case (word, list) =>
        list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
    }
    value.foreach(println)

    println("========")

    val value3 = lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    value3.foreach(println)
  }
}
