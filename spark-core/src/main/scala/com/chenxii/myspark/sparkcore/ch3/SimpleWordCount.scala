package com.chenxii.myspark.sparkcore.ch3

import org.apache.spark.{SparkConf, SparkContext}

// 提交命令
// spark-submit --class com.chenxii.myspark.sparkcore.SimpleWordCount \
// spark-core-1.1.0.jar /home/chenxii/Documents/spark/ch2/datas \
// /home/chenxii/Documents/spark/ch2/outputFile
object SimpleWordCount {

  def main(args: Array[String]): Unit = {
    // 参数校验
    if (args.length < 2) {
      println("用法提示，至少传入两个参数")
      System.exit(0)
    }

    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("SimpleWordCount")
    val sparkContext = new SparkContext(sparkConf)

    val input = sparkContext.textFile(args(0))
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile(args(1))
  }

}
