package com.chenxii.myspark.sparkcore.ch6

/**
 * 玩具
 */
class Toy(name: String, price: Double) extends Serializable {

  var toyName: String = name

  var toyPrice: Double = price

  override def toString: String = {
    "[toyName=\"" + toyName + "\", toyPrice=\"" + toyPrice + "\"]"
  }

}
