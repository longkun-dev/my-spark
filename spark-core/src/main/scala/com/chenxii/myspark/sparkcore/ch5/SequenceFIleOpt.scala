package com.chenxii.myspark.sparkcore.ch5

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读写 sequence file
 */
object SequenceFIleOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("SequenceFileOpt")
    val sparkContext = new SparkContext(sparkConf)

    // 保存 sequence file
//    val inputRDD = sparkContext.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail",  2)))
//    inputRDD.saveAsSequenceFile("datas/ch5/sequence_output")

    val inputRDD1 = sparkContext.sequenceFile("datas/ch5/sequence_file", classOf[Text], classOf[IntWritable])
    inputRDD1.foreach(e => {
      println("姓名: " + e._1, "  ==> 看过数: " + e._2)
    })

    sparkContext.stop()
  }

}
