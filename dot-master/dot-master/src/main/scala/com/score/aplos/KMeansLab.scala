package com.score.aplos

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.functions.udf


object KMeansLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema
  val schema = StructType(
    StructField("time", TimestampType, nullable = true) ::
      StructField("lat", DoubleType, nullable = true) ::
      StructField("lon", DoubleType, nullable = true) ::
      StructField("base", StringType, nullable = true) ::
      Nil
  )

  // read to DataFrame
  val uberDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
    .schema(schema)
    .load(getClass.getResource("/uber.csv").getPath)
    .cache()
  uberDf.printSchema()
  uberDf.show(10)

  // transform userDf with VectorAssembler to add feature column
  val cols = Array("lat", "lon")
  val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
  val featureDf = assembler.transform(uberDf)
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
  predictDf.show(10)

  // calculate distance from center
  val distFromCenter = udf((features: Vector, c: Int) => Vectors.sqdist(features, kmeansModel.clusterCenters(c)))
  val distanceDf = predictDf.withColumn("distance", distFromCenter($"features", $"prediction"))
  distanceDf.show(10)

  // no of categories
  predictDf.groupBy("prediction").count().show()

  // save model
  kmeansModel.write.overwrite()
    .save("/Users/eranga/Workspace/rahasak/labs/spark/models/uber-model")

  // load model
  val kmeansModelLoded = KMeansModel
    .load("/Users/eranga/Workspace/rahasak/labs/spark/models/uber-model")

  // sample data, it could comes via kafka(through spark streams)
  val df1 = Seq(
    ("5/1/2014 0:02:00", 40.7521, -73.9914, "B02512"),
    ("5/1/2014 0:06:00", 40.6965, -73.9715, "B02512"),
    ("5/1/2014 0:15:00", 40.7464, -73.9838, "B02512"),
    ("5/1/2014 0:17:00", 40.7463, -74.0011, "B02512"),
    ("5/1/2014 0:17:00", 40.7594, -73.9734, "B02512")
  ).toDF("time", "lat", "lon", "base")
  df1.show()

  // transform sample data set and add feature column
  val df2 = assembler.transform(df1)
  df2.show()

  // prediction of sample data set with loaded model
  val df3 = kmeansModelLoded.transform(df2)
  df3.show()

}

