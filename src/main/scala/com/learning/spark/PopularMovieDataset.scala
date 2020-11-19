package com.learning.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,LongType,StructType}


object PopularMovieDataset {

  final case class Movie(movieID:Int)

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession
      .builder
      .appName("PopularMovieCount")
      .master("local[*]")
      .getOrCreate()

    val movieSchema = new StructType()
      .add("userID",IntegerType,nullable=true)
      .add("movieID",IntegerType,nullable=true)
      .add("rating",IntegerType,nullable=true)
      .add("timestamp",LongType,nullable=true)

    import spark.implicits._
    val movieDS = spark.read
      .option("sep","\t")
      .schema(movieSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    val topMovie = movieDS.groupBy("movieID").count().orderBy(desc("count"))

    topMovie.show(10)

    spark.stop()
  }

}
