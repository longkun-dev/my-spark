package com.chenxii.myspark.sparkcore.ch5

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * Json 格式文件操作
 */
object JsonFileOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("TextFileOpt")
    val sparkContext = new SparkContext(sparkConf)

    val source = Source.fromFile("datas/ch5/person.json", "UTF-8")
    val lines = source.getLines().toArray
    val personArray = JSON.parseArray(lines(0), classOf[Person]).toArray

//    val result = personArray.filter(person => {
//      !person.asInstanceOf[Person].love
//    })

//    result.foreach(println)

    // 将结果保存为 json
    val input = sparkContext.parallelize(personArray.toList)
    input.filter(p => {
      p.asInstanceOf[Person].love
    }).saveAsTextFile("datas/ch5/json_output")

    println("程序结束")


    sparkContext.stop()
  }

}
