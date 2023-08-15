package com.chenxii.myspark.sparkcore.ch5

case class CsvPerson(name: String, age: String, love: String) {

  override def toString: String = {
    "[name=\"" + name + "\", \"" + age + "\", \"" + love + "\"]"
  }

}
