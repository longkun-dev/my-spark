package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读写各种文件系统
 */
object FileSystemOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("FileSystemOpt")
    val sparkContext = new SparkContext(sparkConf)

    // 读取 hdfs 文件
    val inputRDD = sparkContext.textFile("hdfs://192.168.31.66:9000/chenxii/files/hello-world.md")
    inputRDD.take(10).foreach(println)

    // 将数据写入 hdfs 文件
    val outputRDD = sparkContext.parallelize(Seq("Hello world!"))
    outputRDD.saveAsTextFile("hdfs://192.168.31.66:9000/chenxii/files/spark_hdfs_output")

    sparkContext.stop()
  }
}
