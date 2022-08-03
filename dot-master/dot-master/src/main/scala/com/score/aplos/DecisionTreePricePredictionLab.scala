package com.score.aplos

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * https://github.com/oleksandrkim/Spark-Scala-predict-price-of-a-diamond-with-decision-tree-and-random-forest
 */
object DecisionTreePricePredictionLab extends App {

  case class Flight(_id: String, dofW: Int, carrier: String, origin: String, dest: String, crsdephour: Int, crsdeptime: Double,
                    depdelay: Double, crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dis: Double)

  case class Price(carot: Double, x: Double, y: Double, z: Double)

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits

  // schema of uber
  val schema = StructType(
    StructField("id", StringType, nullable = true) ::
      StructField("carat", DoubleType, nullable = true) ::
      StructField("cut", StringType, nullable = true) ::
      StructField("color", StringType, nullable = true) ::
      StructField("clarity", StringType, nullable = true) ::
      StructField("depth", DoubleType, nullable = true) ::
      StructField("table", DoubleType, nullable = true) ::
      StructField("price", DoubleType, nullable = true) ::
      StructField("x", DoubleType, nullable = true) ::
      StructField("y", DoubleType, nullable = true) ::
      StructField("z", DoubleType, nullable = true) ::
      Nil
  )

  // read csv
  val diamondDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .load(getClass.getResource("/diamonds.csv").getPath)
    .cache()
  diamondDf.printSchema()
  diamondDf.show(10)

  // price to label
  val labelDf = diamondDf.withColumnRenamed("price", "label")
  labelDf.printSchema()
  labelDf.show(10)

  // indexers to convert cut, color and calrity to integers
  val cutIndexer = new StringIndexer().setInputCol("cut").setOutputCol("cutIndex")
  val colorIndexer = new StringIndexer().setInputCol("color").setOutputCol("colorIndex")
  val clarityIndexer = new StringIndexer().setInputCol("clarity").setOutputCol("clarityIndex")

  // encoder
  val inCols = Array("cutIndex", "colorIndex", "clarityIndex")
  val outCols = Array("cutIndexEnc", "colorIndexEnc", "clarityIndexEnc")
  val encoder = new OneHotEncoderEstimator()
    .setInputCols(inCols)
    .setOutputCols(outCols)

  // vector assembler
  val featureCols = Array("carat", "cutIndexEnc", "colorIndexEnc", "clarityIndexEnc", "depth", "table", "x", "y", "z")
  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features_assem")

  // scaler
  val scaler = new MinMaxScaler().setInputCol("features_assem").setOutputCol("features")

  // decision tree
  val decisionTree = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")

  // pipeline
  // StringIndexer, OneHotEncoderEstimator, VectorAssembler are transformers
  // DecisionTree is the estimator
  val stages = Array(cutIndexer, colorIndexer, clarityIndexer, encoder, assembler, scaler, decisionTree)
  val pipeline = new Pipeline().setStages(stages)

  // param grid
  // decision tree parameters: Max depth(5, 10, 15, 20, 30) and Max Bins(10, 20, 30, 50)
  val maxDepth = Array(5, 10, 15, 20, 30)
  val maxBins = Array(10, 20, 30, 50)
  val paramGrid = new ParamGridBuilder()
    .addGrid(decisionTree.maxDepth, maxDepth)
    .addGrid(decisionTree.maxBins, maxBins)
    .build()

  // cross validator
  val crossValidator = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)

  // split data set training(70%) and test(30%)
  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train and evaluate the mode with data
  val model = crossValidator.fit(trainingData)
  val predictions = model.transform(testData)
  predictions.printSchema()
  predictions.show(10)
  predictions.select("features", "label", "prediction").show()

  // evaluate model
  // Select (prediction, true label) and compute test error.
  // root mean squared error
  val eval1 = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = eval1.evaluate(predictions)
  println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  // r-squared
  val eval2 = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("r2")
  val r2 = eval2.evaluate(predictions)
  println("R-squared (r^2) on test data = " + r2)

  // mean absolute error
  val evaluator_mae = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("mae")
  val mae = evaluator_mae.evaluate(predictions)
  println("Mean Absolute Error (MAE) on test data = " + mae)

  // mean squared error
  val evaluator_mse = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("mse")
  val mse = evaluator_mse.evaluate(predictions)
  println("Mean Squared Error (MSE) on test data = " + mse)

}
