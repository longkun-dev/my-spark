package com.chenxii.myspark.sparkcore.ch6

import org.apache.spark.util.AccumulatorV2

/**
 * 自定义累加器操作对象，求最大值
 */
class MyAccumulator extends AccumulatorV2[Toy, Double] {

  var maxPrice: Double = 0.00

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[Toy, Double] = new MyAccumulator

  override def reset(): Unit = {
    this.maxPrice = 0.00
  }

  override def add(v: Toy): Unit = {
    if (v.toyPrice > this.maxPrice) {
      this.maxPrice = v.toyPrice
    }
    println("maxPrice  ===> ", this.maxPrice)
  }

  override def merge(other: AccumulatorV2[Toy, Double]): Unit = {
    Math.max(this.maxPrice, other.value)
  }

  override def value: Double = this.maxPrice

}
