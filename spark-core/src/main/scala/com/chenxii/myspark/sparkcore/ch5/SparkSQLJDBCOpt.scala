package com.chenxii.myspark.sparkcore.ch5

import org.apache.spark.sql.SparkSession

/**
 * 通过 SparkSQL 读取关系数据库
 */
object SparkSQLJDBCOpt {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkSQLJDBCOpt")
      .getOrCreate()

    val data = sparkSession.read.format("jdbc")
      .option("url", "jdbc:postgresql://192.168.31.66:5432/my_spark")
      .option("driver", "org.postgresql.Driver")
      .option("user", "postgres")
      .option("password", "secret")
      .option("dbtable", "t_customer")
      .load()

    val result = data.select("uid", "name", "sex", "age").filter("age = 20")

    result.collect().toList.foreach(e => {
      println("uid: ", e.getString(0))
      println("name: ", e.getString(1))
      println("sex: ", e.getString(2))
      println("age: ", e.getInt(3))
    })
  }
}
