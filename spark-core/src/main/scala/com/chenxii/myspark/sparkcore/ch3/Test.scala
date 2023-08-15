package com.chenxii.myspark.sparkcore.ch3

object Test {

  def main(args: Array[String]): Unit = {
    val list: List[String] = List("Zhang", "Wang", "Li", "Wu")
    val result = list.map(item => (item.charAt(0), item))
    result.foreach(println)
  }
}
