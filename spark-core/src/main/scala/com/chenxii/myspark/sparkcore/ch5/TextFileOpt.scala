package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 文本文件读取保存等
 */
object TextFileOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("TextFileOpt")
    val sparkContext = new SparkContext(sparkConf)

    val input = sparkContext.wholeTextFiles("datas/ch5/text_file*.txt")
    val result = input.mapValues { lines =>
      lines.split(" ")
    }

    result.foreach { file =>
      println("file - " + file._1)
      println("count: " + file._2.toList.size)
    }

    result.saveAsTextFile("datas/ch5/text_output")

    sparkContext.stop()
  }

}
