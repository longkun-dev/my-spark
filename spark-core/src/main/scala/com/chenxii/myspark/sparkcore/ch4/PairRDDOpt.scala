package com.chenxii.myspark.sparkcore.ch4

import org.apache.spark.{SparkConf, SparkContext}

object PairRDDOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("PairRDDOpt")
      .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val tuple1 = new Tuple2[Int, Int](1, 2)
    val tuple2 = new Tuple2[Int, Int](3, 4)
    val tuple3 = new Tuple2[Int, Int](3, 6)

    val inputRDD = sparkContext.parallelize(List(tuple1, tuple2, tuple3))
    val result1 = inputRDD.reduceByKey((x, y) => x + y)
    result1.foreach(println)

    val result2 = inputRDD.groupByKey()
    result2.foreach(println)

    val result3 = inputRDD.mapValues(x => x * x)
    result3.foreach(println)

    inputRDD.keys.foreach(println)
    inputRDD.values.foreach(println)

    val tuple4 = new Tuple2[Int, Int](3, 9)
    val inputRDD2 = sparkContext.parallelize(List(tuple4))
    val result4 = inputRDD.subtractByKey(inputRDD2)
    result4.foreach(println)

    val result5 = inputRDD.join(inputRDD2)
    result5.foreach(println)
    println("========")
    result5.values.foreach(println)

    val result6 = inputRDD.leftOuterJoin(inputRDD2)
    result6.foreach(println)
    result6.values.foreach(println)

    val inputRDD3 = sparkContext.textFile("datas/README.md")
    val words = inputRDD3.flatMap(line => line.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    result.foreach(println)
    println(result.getNumPartitions)

    val inputRDD4 = sparkContext.parallelize(Seq((4, 5), (2, 3), (3, 4)))
    val result7 = inputRDD4.sortByKey()
    result7.keys.foreach(println)
    println("countByKey: " + inputRDD4.countByKey())
    println("lookup: " + inputRDD4.lookup(4).head)
    println("collectAsMap: " + result7.collectAsMap()(2))

    // 停止
    sparkContext.stop()
  }

}
