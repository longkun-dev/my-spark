package com.chenxii.myspark.sparkcore.ch4

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 控制数据分区减少网络传输以提升性能
 */
object DataPartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("DataPartition")
    val sparkContext = new SparkContext(sparkConf)

    val userSubscribeRDD = sparkContext.textFile("datas/user_subscribe.txt")
      // 避免再次使用该rdd时重复计算分区，所以需要重复使用的话最好持久化
      .persist(StorageLevel.MEMORY_ONLY)
    val userLinkRDD = sparkContext.textFile("datas/user_link.txt", 3)
    val subscribeRDD = userSubscribeRDD.map(line => (line.split(" ")(0), line.split(" ")(1))).groupByKey()
      .partitionBy(new HashPartitioner(4))
    println("partitioner: " + subscribeRDD.partitioner)

    val linkRDD = userLinkRDD.map(line => (line.split(" ")(0), line.split(" ")(1)))
    val joinResult = subscribeRDD.join(linkRDD)
    joinResult.foreach(println)
    println("======")
    // (user_a, ((net_1, net_2), (net_1, net_3)))
    val result = joinResult.filter {
      case (_, (subscribeInfo, linkInfo)) =>
        !subscribeInfo.toList.contains(linkInfo)
    }

    result.foreach(item => {
      println("用户: " + item._1)
      println("订阅地址: " + item._2._1.mkString(","))
      println("访问新地址: " + item._2._2)
      println("=======")
    })
  }

}
