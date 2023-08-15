package com.chenxii.myspark.sparkcore.ch7

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用 spark-submit 在 spark 集群计算单词数据量
 * spark-submit --class com.chenxii.myspark.sparkcore.ch7.WordCountV2
 * --master local --executor-memory 2g spark-core-1.1.0.jar
 *
 * spark-submit --class com.chenxii.myspark.sparkcore.ch7.WordCountV2
 * --master yarn --queue default spark-core-1.1.0.jar
 * --driver-memory 200m --executor-memory 200m --total-executor-memory 1g
 */
object WordCountV2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      // 使用尽可能多的核心
//      .setMaster("local[*]")
      .setMaster("yarn")
      .set("yarn.resourcemanager.hostname", "192.168.31.66")
      .setAppName("WordCountV2")
    val sparkContext = new SparkContext(sparkConf)

//    val inputRDD = sparkContext.textFile("datas/ch7/spark-r.txt")
    val inputRDD = sparkContext.textFile("file:///home/chenxii/Documents/spark/ch7/spark-r.txt")
    val wordCount = inputRDD.flatMap { line =>
      line.split(" ")
    }.distinct()
      .count()

    println("单词数量为: " + wordCount)

    sparkContext.stop()
  }

}
