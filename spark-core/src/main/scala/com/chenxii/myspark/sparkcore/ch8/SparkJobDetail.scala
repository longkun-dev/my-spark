package com.chenxii.myspark.sparkcore.ch8

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 执行的组成部分
 * Job, Task, Stage
 */
object SparkJobDetail {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("SparkJobDetail")
      // 指定序列化格式
//      .set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
//      .set("spark.kyro.registrationRequired", "true")
//      .registerKryoClasses(Array(classOf[Toy], classOf[Person]))
    val sparkContext = new SparkContext(sparkConf)

    val input = sparkContext.textFile("file:///home/chenxii/Documents/spark/ch8/plain_text.txt")
    // 两层Array
    val tokenized = input.map(line => line.split(" ")(0))
      .filter(_.nonEmpty)
    val counts = tokenized.map(words => (words, 1))
      .reduceByKey { (a, b) => a + b }

    println("消息统计:")

    counts.foreach(println)

    println("input: ")
    println(input.toDebugString)

    println("counts: ")
    println(counts.toDebugString)

    sparkContext.stop()
  }
}
