package com.score.aplos

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions.{col,hour,minute,second}


object KMeansTaxiLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._
  val taxiRidesSchema = StructType(Array(
      StructField("rideId", LongType), StructField("isStart", StringType),
      StructField("endTime", TimestampType), StructField("startTime", TimestampType),
      StructField("startLon", FloatType), StructField("startLat", FloatType),
      StructField("endLon", FloatType), StructField("endLat", FloatType),
      StructField("passengerCnt", ShortType), StructField("taxiId", LongType),
      StructField("driverId", LongType)))

  val taxiFaresSchema = StructType(Seq(
      StructField("rideId", LongType), StructField("taxiId", LongType),
      StructField("driverId", LongType), StructField("startTime", TimestampType),
      StructField("paymentType", StringType), StructField("tip", FloatType),
      StructField("tolls", FloatType), StructField("totalFare", FloatType)))

  // read to ride DataFrame
  val rideDf = spark.read.format("csv")
    .option("header", value = false)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
    .schema(taxiRidesSchema)
    .load(getClass.getResource("/nycTaxiRides.gz").getPath)
    .cache()
    .as("rides")
  rideDf.printSchema()
  rideDf.show(10)

  // read to ride DataFrame
  val fareDf = spark.read.format("csv")
    .option("header", value = false)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
    .schema(taxiFaresSchema)
    .load(getClass.getResource("/nycTaxiFares.gz").getPath)
    .cache()
    .as("fares")
  fareDf.printSchema()
  fareDf.show(10)

  val vectCol = udf((tip: Double) => Vectors.dense(tip))
  val df = rideDf
    .join(fareDf, $"rides.rideId" === $"fares.rideId")
    .filter($"tip" > 0)

  df.printSchema()
  df.show(10)

  val df1 = df.withColumn("tipVect", vectCol(df("tip")))
      .withColumn("startHour", hour(col("startTime")))

  df1.printSchema()
  df1.show(10)

  //df.filter($"tip" > 5).show(10)

  df1.select("tip").describe().show()

  //val df1 = df.filter($"tip" > 5)
}
