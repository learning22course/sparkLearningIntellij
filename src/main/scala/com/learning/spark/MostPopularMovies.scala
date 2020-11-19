package com.learning.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,StructType,StringType}

object MostPopularMovies {

  case class SuperHeroNames(id:Int,name:String)
  case class SuperHero(value:String)

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MostPopularNames")
      .master("local[*]")
      .getOrCreate()

    val NameSchema = new StructType()
      .add("id",IntegerType,nullable = true)
      .add("name",StringType,nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(NameSchema)
      .option("sep"," ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id",split(col("value")," ")(0))
      .withColumn("connections",size(split(col("value")," ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val mostpopular = connections
      .sort($"connections".desc)
      .first()

    //mostpopular.show()
    //println(mostpopular)

    val mostPopularNames = names
      .filter($"id" === mostpopular(0))
      .select("name")
      .first()

    println(s"${mostPopularNames(0)} is the most popular superhero with ${mostpopular(1)} co-appearances.")

    val minConnectionCount = connections.agg(min("connections")).first().getLong(0)

    val minConnections = connections.filter($"connections" === minConnectionCount)

    val minConnectionWithNames = minConnections.join(names,usingColumn="id")

    println("The Following characters have only  "+ minConnectionCount+ " connection(s):")

    minConnectionWithNames.select("name").show()

    spark.stop()

  }

}
