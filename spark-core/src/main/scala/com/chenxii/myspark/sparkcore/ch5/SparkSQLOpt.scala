package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.sql.SparkSession

/**
 * 通过 spark-sql 连接 hive
 */
object SparkSQLOpt {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      // 一定要先启动: hive --service metastore &
      // 重要，一定要设置才能读取 hive 库信息
      .config("hive.metastore.uris", "thrift://192.168.31.66:9083")
      .getOrCreate()

    sparkSession.sql("show tables in jinghong").show()

    println("=====")

    val rows = sparkSession.sql("select * from jinghong.rate where rate > 2")
    val firstRow = rows.first()
    // 读取第一行的第一个字段值
    println(firstRow.getString(0))

    sparkSession.stop()
  }

}
