package com.chenxii.myspark.sparkcore.ch4

import org.apache.spark.Partitioner

import java.net.URL

/**
 * 自定义分区器
 */
class MyPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new URL(key.toString).getHost
    val code = domain.hashCode % numPartitions
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case dnp: MyPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

}
