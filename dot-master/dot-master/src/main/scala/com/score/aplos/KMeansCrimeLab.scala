package com.score.aplos
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KMeansCrimeLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema of uber
  val schema = StructType(
    StructField("state", StringType, nullable = true) ::
        StructField("code", IntegerType, nullable = true) ::
      StructField("murder", DoubleType, nullable = true) ::
      StructField("assult", DoubleType, nullable = true) ::
      StructField("urbanpop", DoubleType, nullable = true) ::
      StructField("rape", DoubleType, nullable = true) ::
      Nil
  )

  // read crime.csv file to DataFrame
  val crimeDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .load(getClass.getResource("/crime.csv").getPath)
    .cache()
  crimeDf.show(100)
  crimeDf.printSchema()

  // transform crimeDf with VectorAssembler to add feature column
  val cols = Array("murder", "assult", "urbanpop", "rape")
  val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
  val featureDf = assembler.transform(crimeDf)
  featureDf.printSchema()
  featureDf.show(10)

  // split data set training(70%) and test(30%)
  val seed = 5043
  val Array(trainingData, testData) = featureDf.randomSplit(Array(0.7, 0.3), seed)

  // kmeans model with 8 clusters
  val kmeans = new KMeans()
    .setK(8)
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
  val kmeansModel = kmeans.fit(trainingData)
  kmeansModel.clusterCenters.foreach(println)

  // test the model with test data set
  val predictDf = kmeansModel.transform(testData)
  predictDf.show()

  // no of categories
  predictDf.groupBy("prediction").count().show()
}

