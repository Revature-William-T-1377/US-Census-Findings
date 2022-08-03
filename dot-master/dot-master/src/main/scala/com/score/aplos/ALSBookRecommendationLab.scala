package com.score.aplos

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object ALSBookRecommendationLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // book schema
  val bookSchema = StructType(
    StructField("ISBN", StringType, nullable = true) ::
      StructField("Title", StringType, nullable = true) ::
      StructField("Author", StringType, nullable = true) ::
      StructField("Year", IntegerType, nullable = true) ::
      StructField("Publisher", StringType, nullable = true) ::
      Nil
  )

  // rating schema
  val ratingSchema = StructType(
    StructField("USER-ID", IntegerType, nullable = true) ::
      StructField("ISBN", IntegerType, nullable = true) ::
      StructField("Rating", IntegerType, nullable = true) ::
      Nil
  )

  // read books
  val bookDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ";")
    .option("mode", "DROPMALFORMED")
    .schema(bookSchema)
    .load(getClass.getResource("/rec_books.csv").getPath)
    .cache()
    .as("books")
  bookDf.printSchema()
  bookDf.show(10)

  // read ratings
  val ratingDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ";")
    .option("mode", "DROPMALFORMED")
    .schema(ratingSchema)
    .load(getClass.getResource("/rec_ratings.csv").getPath)
    .cache()
    .as("ratings")
  ratingDf.printSchema()
  ratingDf.show(10)

  // join dfs
  val jdf = ratingDf.join(bookDf, $"ratings.ISBN" === $"books.ISBN")
    .select(
      $"ratings.USER-ID".as("userId"),
      $"ratings.Rating".as("rating"),
      $"ratings.ISBN".as("isbn"),
      $"books.Title".as("title"),
      $"books.Author".as("author"),
      $"books.Year".as("year"),
      $"books.Publisher".as("publisher")
    )
  jdf.printSchema()
  jdf.show(10)

  // do some filtering for test
  jdf.filter(col("userId") === "277378")
    .limit(5)
    .show()

  // split data set training(70%) and test(30%)
  val Array(trainingData, testData) = jdf.randomSplit(Array(0.7, 0.3))

  // build recommendation model with als algorithm
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("isbn")
    .setRatingCol("rating")
  val alsModel = als.fit(trainingData)

  // evaluate the als model
  // compute root mean square error(rmse) with test data for evaluation
  // set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
  alsModel.setColdStartStrategy("drop")
  val predictions = alsModel.transform(testData)
  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"root mean square error $rmse")

  // top 10 book recommendations for each user
  val allUserRec = alsModel.recommendForAllUsers(10)
  allUserRec.printSchema()
  allUserRec.show(10)

  // top 10 user recommendations for each book
  val allBookRec = alsModel.recommendForAllItems(10)
  allBookRec.printSchema()
  allBookRec.show(10)

  // top 10 book recommendations for specific set of users(3 users)
  val userRec = alsModel.recommendForUserSubset(jdf.select("userId").distinct().limit(3), 10)
  userRec.printSchema()
  userRec.show(10)

  // top 10 user recommendations for specific set of books(3 books)
  val bookRec = alsModel.recommendForItemSubset(jdf.select("isbn").distinct().limit(3), 10)
  bookRec.printSchema()
  bookRec.show(10)

  // top 10 book recommendations for user 277378
  val udf = jdf.select("userId").filter(col("userId") === 277378).limit(1)
  val userRec277378 = alsModel.recommendForUserSubset(udf, 10)
  userRec277378.printSchema()
  userRec277378.show(10)

}
