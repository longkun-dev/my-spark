package com.chenxii.myspark.sparkcore.ch5

case class Person(name: String, love: Boolean) {

  override def toString: String = {
    "{name=\"" + name + "\", love=\"" + love + "\"}"
  }

}
