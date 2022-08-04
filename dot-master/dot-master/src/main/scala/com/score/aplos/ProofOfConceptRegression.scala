package com.score.aplos

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ProofOfConceptRegression extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema
  val schema = StructType(
    StructField("Pop1", IntegerType, nullable = true) ::
      StructField("Pop2", IntegerType, nullable = true) ::
      StructField("result", IntegerType, nullable = true) ::
      Nil
  )

  // read to DataFrame
  val marksDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .load(getClass.getResource("/scores.csv").getPath)
    .cache()
  marksDf.printSchema()
  marksDf.show(10)
  marksDf.describe("Pop1").show()

  // columns that need to added to feature column
  val cols = Array("Pop1", "Pop2")

  // VectorAssembler to add feature column
  // input columns - cols
  // feature column - features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
  val featureDf = assembler.transform(marksDf)
  featureDf.printSchema()
  featureDf.show(10)

  // StringIndexer define new 'label' column with 'result' column
  val indexer = new StringIndexer()
    .setInputCol("result")
    .setOutputCol("label")
  val labelDf = indexer.fit(featureDf).transform(featureDf)
  labelDf.printSchema()
  labelDf.show(10)

  // split data set training and test
  // training data set - 70%
  // test data set - 30%
  val seed = 5043
  val Array(trainingData, testData) = labelDf.randomSplit(Array(0.7, 0.3), seed)

  // train logistic regression model with training data set
  val logisticRegression = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.02)
    .setElasticNetParam(0.8)
  val logisticRegressionModel = logisticRegression.fit(trainingData)

  // run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction
  val predictionDf = logisticRegressionModel.transform(testData)
  predictionDf.show(10)

  // we run marksDf on the pipeline, so split marksDf
  val Array(pipelineTrainingData, pipelineTestingData) = marksDf.randomSplit(Array(0.7, 0.3), seed)

  // VectorAssembler and StringIndexer are transformers
  // LogisticRegression is the estimator
  val stages = Array(assembler, indexer, logisticRegression)

  // build pipeline
  val pipeline = new Pipeline().setStages(stages)
  val pipelineModel = pipeline.fit(pipelineTrainingData)

  // test model with test data
  val pipelinePredictionDf = pipelineModel.transform(pipelineTestingData)
  pipelinePredictionDf.show(10)

  // evaluate model with area under ROC
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(predictionDf)
  println(accuracy)

  // measure the accuracy of pipeline model
  val pipelineAccuracy = evaluator.evaluate(pipelinePredictionDf)
  println(pipelineAccuracy)

  // save model
  logisticRegressionModel.write.overwrite()
    .save("/Users/Corey/Workspace/LogisticRegressionTest")

  // load model
  val logisticRegressionModelLoaded = LogisticRegressionModel
    .load("/Users/Corey/Workspace/LogisticRegressionTest")

  // sample data, it could comes via kafka(through spark streams)
  val df1 = Seq(
    (4779736, 5024279),
    (710231, 733391),
    (6392017, 7151502),
    (2915918, 3011524),
    (37253956, 39538223)
  ).toDF("Pop1", "Pop2")
  df1.show()

  // transform sample data set and add feature column
  val df2 = assembler.transform(df1)
  df2.show()

  // prediction of pass/fail status of sample data set
  val df3 = logisticRegressionModelLoaded.transform(df2)
  df3.show()
}
