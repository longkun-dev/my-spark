package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.sql.SparkSession

/**
 * 通过 sparkSession 读取 json 格式文件
 */
object SparkSQLJsonOpt {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkSQLJsonOpt")
      .getOrCreate()

    sparkSession.read.format("json")
      .option("primitiveAsString", "true")
      .load("datas/ch5/json_file")
      // 类似表
      .createOrReplaceTempView("t_tweets")

    sparkSession.sql("select user, text from t_tweets").show()

    sparkSession.stop()
  }
}
