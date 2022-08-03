package com.score.aplos

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RandomForestLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema
  val schema = StructType(
    StructField("creditability", DoubleType, nullable = true) ::
      StructField("balance", DoubleType, nullable = true) ::
      StructField("duration", IntegerType, nullable = true) ::
      StructField("history", IntegerType, nullable = true) ::
      StructField("purpose", IntegerType, nullable = true) ::
      StructField("amount", IntegerType, nullable = true) ::
      StructField("savings", IntegerType, nullable = true) ::
      StructField("employment", IntegerType, nullable = true) ::
      StructField("instPercent", IntegerType, nullable = true) ::
      StructField("sexMarried", IntegerType, nullable = true) ::
      StructField("guarantors", IntegerType, nullable = true) ::
      StructField("residenceDuration", IntegerType, nullable = true) ::
      StructField("assets", IntegerType, nullable = true) ::
      StructField("age", IntegerType, nullable = true) ::
      StructField("concCredit", IntegerType, nullable = true) ::
      StructField("apartment", IntegerType, nullable = true) ::
      StructField("credits", IntegerType, nullable = true) ::
      StructField("occupation", IntegerType, nullable = true) ::
      StructField("dependents", IntegerType, nullable = true) ::
      StructField("hasPhone", IntegerType, nullable = true) ::
      StructField("foreign", IntegerType, nullable = true) ::
      Nil
  )

  // read to DataFrame
  val creditDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .load(getClass.getResource("/credit.csv").getPath)
    .cache()
  creditDf.printSchema()
  creditDf.show(10)
  creditDf.describe("balance").show()

  // columns that need to added to feature column
  val cols = Array("balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPercent", "sexMarried",
    "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone",
    "foreign")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val featureDf = assembler.transform(creditDf)
  featureDf.printSchema()
  featureDf.show(10)

  // StringIndexer define new 'label' column with 'result' column
  val indexer = new StringIndexer()
    .setInputCol("creditability")
    .setOutputCol("label")
  val labelDf = indexer.fit(featureDf).transform(featureDf)
  labelDf.printSchema()
  labelDf.show(10)

  // split data set training and test
  // training data set - 70%
  // test data set - 30%
  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train Random Forest model with training data set
  val randomForestClassifier = new RandomForestClassifier()
    .setImpurity("gini")
    .setMaxDepth(3)
    .setNumTrees(20)
    .setFeatureSubsetStrategy("auto")
    .setSeed(seed)
  val randomForestModel = randomForestClassifier.fit(trainingData)
  println(randomForestModel.toDebugString)

  // run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction
  val predictionDf = randomForestModel.transform(testData)
  predictionDf.show(10)

  // we run creditDf on the pipeline, so split creditDf
  val Array(pipelineTrainingData, pipelineTestingData) = creditDf.randomSplit(Array(0.7, 0.3), seed)

  // VectorAssembler and StringIndexer are transformers
  // LogisticRegression is the estimator
  val stages = Array(assembler, indexer, randomForestClassifier)

  // build pipeline
  val pipeline = new Pipeline().setStages(stages)
  val pipelineModel = pipeline.fit(pipelineTrainingData)

  // test model with test data
  val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
  pipelinePredictionDf.show(10)

  // evaluate model with area under ROC
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setMetricName("areaUnderROC")
    .setRawPredictionCol("rawPrediction")

  // measure the accuracy
  val accuracy = evaluator.evaluate(predictionDf)
  println(accuracy)

  // measure the accuracy of pipeline model
  val pipelineAccuracy = evaluator.evaluate(pipelinePredictionDf)
  println(pipelineAccuracy)

  // parameters that needs to tune, we tune
  //  1. max buns
  //  2. max depth
  //  3. impurity
  val paramGrid = new ParamGridBuilder()
    .addGrid(randomForestClassifier.maxBins, Array(25, 28, 31))
    .addGrid(randomForestClassifier.maxDepth, Array(4, 6, 8))
    .addGrid(randomForestClassifier.impurity, Array("entropy", "gini"))
    .build()

  // define cross validation stage to search through the parameters
  // K-Fold cross validation with BinaryClassificationEvaluator
  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(5)

  // fit will run cross validation and choose the best set of parameters
  // this will take some time to run
  val cvModel = cv.fit(pipelineTrainingData)

  // test cross validated model with test data
  val cvPredictionDf = cvModel.transform(pipelineTestingData)
  cvPredictionDf.show(10)

  // measure the accuracy of cross validated model
  // this model is more accurate than the old model
  val cvAccuracy = evaluator.evaluate(cvPredictionDf)
  println(cvAccuracy)

  // save model
  cvModel.write.overwrite()
    .save("/Users/eranga/Workspace/rahasak/labs/spark/models/credit-model")

  // load CrossValidatorModel model here
  val cvModelLoaded = CrossValidatorModel
    .load("/Users/eranga/Workspace/rahasak/labs/spark/models/credit-model")

  // sample data, it could comes via kafka(through spark streams)
  val df1 = Seq(
    (1, 18, 2, 6, 750, 1, 1, 4, 2, 1, 1, 1, 27, 3, 2, 1, 1, 1, 1, 1),
    (2, 24, 2, 1, 12579, 1, 5, 4, 2, 1, 2, 4, 44, 3, 3, 1, 4, 1, 2, 1),
    (1, 18, 2, 1, 7511, 5, 5, 1, 3, 1, 4, 2, 51, 3, 3, 1, 3, 2, 2, 1),
    (1, 18, 4, 0, 3966, 1, 5, 1, 2, 1, 4, 1, 33, 1, 1, 3, 3, 1, 2, 1),
    (1, 12, 0, 3, 6199, 1, 3, 4, 3, 1, 2, 2, 28, 3, 1, 2, 3, 1, 2, 1),
    (1, 24, 2, 3, 1987, 1, 3, 2, 3, 1, 4, 1, 21, 3, 1, 1, 2, 2, 1, 1),
    (1, 24, 2, 0, 2303, 1, 5, 4, 3, 2, 1, 1, 45, 3, 2, 1, 3, 1, 1, 1),
    (4, 21, 4, 0, 12680, 5, 5, 4, 3, 1, 4, 4, 30, 3, 3, 1, 4, 1, 2, 1),
    (2, 12, 2, 3, 6468, 5, 1, 2, 3, 1, 1, 4, 52, 3, 2, 1, 4, 1, 2, 1),
    (1, 30, 2, 2, 6350, 5, 5, 4, 3, 1, 4, 2, 31, 3, 2, 1, 3, 1, 1, 1)
  ).toDF("balance", "duration", "history", "purpose", "amount", "savings", "employment", "instPercent", "sexMarried",
    "guarantors", "residenceDuration", "assets", "age", "concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone",
    "foreign")
  df1.show()

  // prediction of credit risk of sample data set
  // we don't need to add feature column to data frame since model comes with pipeline
  // pipeline already have VectorAssembler
  val df2 = cvModelLoaded.transform(df1)
  df2.show()
}
