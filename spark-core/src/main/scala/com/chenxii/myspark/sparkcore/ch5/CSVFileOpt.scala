package com.chenxii.myspark.sparkcore.ch5

import com.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{StringReader, StringWriter}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * csv 文件操作
 */
object CSVFileOpt {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("CSVFileOpt")
    val sparkContext = new SparkContext(sparkConf)

    val input = sparkContext.wholeTextFiles("datas/ch5/csv_file.csv")
    val result = input.flatMap { case (_, txt) =>
      val reader = new CSVReader(new StringReader(txt))
      // 根据序号读取 csv 文件的列
      reader.readAll().map(cols => CsvPerson(cols(0), cols(1), cols(2)))
    }

    result.foreach(println)

//    result.map(p => List(p.name, p.age).toArray)
//      .mapPartitions(p => {
//        val stringWriter = new StringWriter()
//        val CSVWriter = new CSVWriter(stringWriter)
//        // FIXME 报错未解决
//        CSVWriter.writeAll(p.toList)
//      }).saveAsTextFile("datas/ch5/csv_output")

    sparkContext.stop()
  }

}
