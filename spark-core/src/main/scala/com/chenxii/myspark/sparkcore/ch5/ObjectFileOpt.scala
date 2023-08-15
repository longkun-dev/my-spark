package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读写对象文件
 */
object ObjectFileOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("ObjectFileOpt")
    val sparkContext = new SparkContext(sparkConf)

//    val inputRDD = sparkContext.parallelize(List(("A", 1), ("B", 2), ("C", 3)))
//    inputRDD.saveAsObjectFile("datas/ch5/object_output")

    val inputRDD1 = sparkContext.objectFile("datas/ch5/object_file")
    inputRDD1.foreach(println)

    sparkContext.stop()
  }

}
