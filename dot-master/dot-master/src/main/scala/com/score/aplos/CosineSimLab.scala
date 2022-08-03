package com.score.aplos

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object CosineSimLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // movie schema
  val movieSchema = StructType(
    StructField("budget", IntegerType, nullable = true) ::
      StructField("genres", StringType, nullable = true) ::
      StructField("homepage", StringType, nullable = true) ::
      StructField("id", IntegerType, nullable = true) ::
      StructField("keywords", StringType, nullable = true) ::
      StructField("original_language", StringType, nullable = true) ::
      StructField("original_title", StringType, nullable = true) ::
      StructField("overview", StringType, nullable = true) ::
      StructField("popularity", DoubleType, nullable = true) ::
      StructField("production_companies", StringType, nullable = true) ::
      StructField("production_countries", StringType, nullable = true) ::
      StructField("release_date", StringType, nullable = true) ::
      StructField("revenue", IntegerType, nullable = true) ::
      StructField("runtime", IntegerType, nullable = true) ::
      StructField("spoken_languages", StringType, nullable = true) ::
      StructField("status", StringType, nullable = true) ::
      StructField("tagline", StringType, nullable = true) ::
      StructField("title", StringType, nullable = true) ::
      StructField("vote_average", DoubleType, nullable = true) ::
      StructField("vote_count", IntegerType, nullable = true) ::
      Nil
  )

  // read movies
  val movieDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(movieSchema)
    .load(getClass.getResource("/tmdb_5000_movies.csv").getPath)
    .cache()
    .as("movies")
  movieDf.printSchema()
  movieDf.show(10)

  movieDf.select("genres").show(5)

//  // rating schema
//  val ratingSchema = StructType(
//    StructField("USER-ID", IntegerType, nullable = true) ::
//      StructField("ISBN", IntegerType, nullable = true) ::
//      StructField("Rating", IntegerType, nullable = true) ::
//      Nil
//  )

//  // read ratings
//  val ratingDf = spark.read.format("csv")
//    .option("header", value = true)
//    .option("delimiter", ";")
//    .option("mode", "DROPMALFORMED")
//    .schema(ratingSchema)
//    .load(getClass.getResource("/rec_ratings.csv").getPath)
//    .cache()
//    .as("ratings")
//  ratingDf.printSchema()
//  ratingDf.show(10)

//  // join dfs
//  val jdf = ratingDf.join(bookDf, $"ratings.ISBN" === $"books.ISBN")
//    .select(
//      $"ratings.USER-ID".as("userId"),
//      $"ratings.Rating".as("rating"),
//      $"ratings.ISBN".as("isbn"),
//      $"books.Title".as("title"),
//      $"books.Author".as("author"),
//      $"books.Year".as("year"),
//      $"books.Publisher".as("publisher")
//    )
//  jdf.printSchema()
//  jdf.show(10)
//
//  // do some filtering for test
//  jdf.filter(col("userId") === "277378")
//    .limit(5)
//    .show()

}
