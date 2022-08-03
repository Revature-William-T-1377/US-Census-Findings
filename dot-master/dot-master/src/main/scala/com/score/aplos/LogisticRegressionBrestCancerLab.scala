package com.score.aplos

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, _}

object LogisticRegressionBrestCancerLab extends App {
  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // schema of the file
  val schema = StructType(
    StructField("STR", DoubleType, nullable = true) ::
      StructField("OSB", DoubleType, nullable = true) ::
      StructField("AGMT", DoubleType, nullable = true) ::
      StructField("FNDX", DoubleType, nullable = true) ::
      StructField("HIGD", DoubleType, nullable = true) ::
      StructField("DEG", DoubleType, nullable = true) ::
      StructField("CHK", DoubleType, nullable = true) ::
      StructField("AGP1", DoubleType, nullable = true) ::
      StructField("AGMN", DoubleType, nullable = true) ::
      StructField("NLM", DoubleType, nullable = true) ::
      StructField("LIV", DoubleType, nullable = true) ::
      StructField("WT", DoubleType, nullable = true) ::
      StructField("AGLP", DoubleType, nullable = true) ::
      StructField("MST", DoubleType, nullable = true) ::
      Nil
  )

  // lambda csv file
  // remove header from it
  val lambdaText = spark.sparkContext.textFile(getClass.getResource("/brest-cancer.csv").getPath)
    .filter(p => !p.contains("STR"))
    .cache()

  // split rows in text by comma
  // remove \" characters
  // remove empty item contains rows
  // convert to double
  val rowRDD = lambdaText.map(p => p.split(",").toList)
    .map(_.map(_.replace("\"", "").trim))
    .filter(!_.contains(""))
    .map(_.map(p => if (p.isEmpty) -1 else p.toDouble))
    .map(p => Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13)))

  // data frame with schema
  val dataFrame1 = spark.createDataFrame(rowRDD, schema)
  dataFrame1.show()

  // features are the column headers
  val features = Array("STR", "OSB", "AGMT", "HIGD", "DEG", "CHK", "AGP1", "AGMN", "NLM", "LIV", "WT", "AGLP", "MST")

  // assembler with new data frame
  val assembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol("features")
  val dataFrame2 = assembler.transform(dataFrame1)
  dataFrame2.show()

  // string indexer to new data frame
  val indexer = new StringIndexer()
    .setInputCol("FNDX")
    .setOutputCol("label")
  val dataFrame3 = indexer.fit(dataFrame2).transform(dataFrame2)
  dataFrame3.show()

  // logistic regression
  val model = new LogisticRegression().fit(dataFrame3)
  val predictions = model.transform(dataFrame3)
  predictions.show()

  predictions.select("features", "label", "prediction").show()
}
