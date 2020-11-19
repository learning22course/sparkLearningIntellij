package com.learning.spark

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col,udf}
import org.apache.spark.sql.types.{IntegerType,LongType,StructType}

import scala.io.{Codec,Source}

object PopularMovieNicerDataset {

  case class Movies(userId:Int,movieId:Int,rating:Int,timestamp:Long)

  def LoadMovieNames() : Map[Int,String] = {

    implicit val codec:Codec = Codec("ISO-8859-1")

    var movieNames:Map[Int,String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for(line <- lines.getLines()){
      val fields = line.split('|')
      if(fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieNames
  }
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("PopularMovieNamesNicer")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(LoadMovieNames())

    val movieSchema = new StructType()
      .add("userId",IntegerType,true)
      .add("movieId",IntegerType,true)
      .add("rating",IntegerType,true)
      .add("timestamp",LongType,true)

    import spark.implicits._
    val movies = spark.read
      .option("sep","\t")
      .schema(movieSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val movieCounts = movies.groupBy("movieId").count()

    val lookupName : Int => String = (movieId:Int)=>{
      nameDict.value(movieId)
    }

    val lookupNameUDF = udf(lookupName)

    val movieWithNames = movieCounts.withColumn("movieTitle",lookupNameUDF(col("movieId")))

    val sortMovieWithNames = movieWithNames.sort("count")
    sortMovieWithNames.show(sortMovieWithNames.count.toInt, truncate = false)

    spark.close()
  }

}
