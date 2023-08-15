package com.chenxii.myspark.sparkstreaming.ch10

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * spark-streaming
 * nc -l -p 7777
 * spark-submit --class ... spark-streaming-1.1.0.jar
 */
object SparkStreamingOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreaming1")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    val lines = streamingContext.socketTextStream("192.168.31.66", 7777, StorageLevel.MEMORY_ONLY)
    val linesWindow = lines.window(Seconds(30), Seconds(20))
    val errorLines = linesWindow.filter { e =>
      e.contains("error")
    }

    errorLines.print()

    errorLines.saveAsTextFiles("file:///home/chenxii/Documents/spark/ch10/result-", "-log")

    // 启动流计算环境并等待其完成
    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
