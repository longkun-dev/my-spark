package com.chenxii.myspark.sparksql.ch9

import org.apache.spark.sql.SparkSession

/**
 * 从 hive 表中读取数据
 * 使用自定义 UDF
 */
object HiveTableRead {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HiveTableRead")
      .config("hive.metastore.uris", "thrift://192.168.31.66:9083")
      .enableHiveSupport()
      .getOrCreate()

    val nationBase = sparkSession.sql("select * from my_spark.nation_base order by id desc")
    val nationCodes = nationBase.rdd.map(row => row.getString(1))
    nationCodes.foreach(println)

    sparkSession.sql("add jar spark-sql/target/spark-sql-1.1.0.jar ")
    sparkSession.sql("create temporary function shield_client_name as 'com.chenxii.myspark.sparksql.ch9.ShieldClientName'")
    sparkSession.sql("select client_name, shield_client_name(client_name) from my_spark.client_info where id < 4;").show()


    sparkSession.stop()
  }

}
