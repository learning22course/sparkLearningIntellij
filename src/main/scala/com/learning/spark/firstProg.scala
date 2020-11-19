package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object firstProg {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","FirstProg")

    val lines = sc.textFile("data/ml-100k/u.data")
    val numLines = lines.count()

    println("the files has  "+numLines+" lines")

  }

}
