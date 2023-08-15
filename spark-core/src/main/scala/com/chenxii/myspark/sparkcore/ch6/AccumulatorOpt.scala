package com.chenxii.myspark.sparkcore.ch6

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark 累加器
 */
object AccumulatorOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("AccumulatorOpt")
    val sparkContext = new SparkContext(sparkConf)

    val inputRDD = sparkContext.textFile("datas/ch6/blank_accumulator_file.txt")
    val blankLines = sparkContext.longAccumulator("blank accumulator")
    val result = inputRDD.flatMap(line => {
      if (line == null || line == "") {
        blankLines.add(1)
      }
      line.split(" ")
    })

    // 0
    println("blank line 1: " + blankLines.value)

    result.saveAsTextFile("datas/ch6/blank_accumulator_output")

    // 3 行动操作触发计算
    println("blank line 2: " + blankLines.value)

    sparkContext.stop()
  }

}
