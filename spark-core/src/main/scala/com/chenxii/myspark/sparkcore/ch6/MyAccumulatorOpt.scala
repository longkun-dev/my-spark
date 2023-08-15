package com.chenxii.myspark.sparkcore.ch6

import org.apache.spark.sql.SparkSession


/**
 * 自定义累加器求对象集合最大值
 * 累加器：只写不读
 */
object MyAccumulatorOpt {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MyAccumulatorOpt")
      .getOrCreate()

    val inputRDD = sparkSession.read
      .format("json")
      .load("datas/ch6/toy_json_file.json")
      .rdd

    val sparkContext = sparkSession.sparkContext

    val myAccumulator: MyAccumulator = new MyAccumulator
    // 注册自定义累加器
    sparkContext.register(myAccumulator)

    inputRDD.foreach(row => {
      val toy: Toy = new Toy(row.getString(0), row.getDouble(1))
      myAccumulator.add(toy)
    })

    // 为什么返回0 ?
    println("自定义累加器结果: " + myAccumulator.value)

    sparkContext.stop()
    sparkSession.stop()
  }

}
