package com.score.aplos

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object LogisticRegressionTumorCancerLab extends App {

  case class Tumor(clas: Double, thickness: Double, size: Double, shape: Double, madh: Double, epsize: Double,
                   bnuc: Double, bchrom: Double, nNuc: Double, mit: Double)

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // tumor-cancer.csv csv file
  // remove 1st column
  // remove rows which contains ?
  val bdaDf = spark.sparkContext.textFile(getClass.getResource("/tumor-cancer.csv").getPath)
    .map(p => p.split(","))
    .map(_.drop(1))
    .filter(!_.contains("?"))
    .map(_.map(_.toDouble))
    .map(p => Tumor(if (p(9) == 4.0) 1 else 0, p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8)))
    .toDF()
    .cache()

  // show df
  bdaDf.printSchema()
  bdaDf.show(10)
  bdaDf.describe("thickness").show()

  // query df
  bdaDf.select("clas", "thickness").show(10)
  bdaDf.groupBy("clas").avg("thickness", "shape", "size").show()

  // create temp view to query with sql
  // sql query from tem view
  bdaDf.createOrReplaceTempView("obs")
  spark.sqlContext.sql("SELECT clas, thickness, size, shape from obs").show()
  spark.sqlContext.sql("SELECT clas, avg(thickness), avg(size), avg(shape) from obs group by clas").show()

  // columns that need to added to feature column
  val cols = Array("thickness", "size", "shape", "madh", "epsize", "bnuc", "bchrom", "nNuc", "mit")

  // VectorAssembler to assemble new 'features' column with feature columns
  // input columns -> cols
  // output column -> features
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")

  // transform bdaDf with assembler to add feature column
  val feDf = assembler.transform(bdaDf)
  feDf.show(10)

  // StringIndexer define new 'label' column with 'clas' column
  // input column -> clas
  // output column -> label
  val indexer = new StringIndexer()
    .setInputCol("clas")
    .setOutputCol("label")

  // transform feDf with indexer to add label column
  val laDf = indexer.fit(feDf).transform(feDf)
  laDf.show(10)

  // split data set training and test
  // training data set - 70%
  // test data set - 30%
  val seed = 5043
  val Array(trainingData, testData) = laDf.randomSplit(Array(0.7, 0.3), seed)

  // train the logistic regression model with elastic net regularization
  val logisticRegression = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
  val model = logisticRegression.fit(trainingData)
  println(s"coefficient: ${model.coefficients}, intercept: ${model.intercept}")

  // test model, run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction
  val prDf = model.transform(testData)
  prDf.show(100)

  // evaluate predictions
  // common metric used for logistic regression is Area Under the ROC Curve (AUC)
  // use BinaryClassificationEvaluator to obtain AUC,
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(prDf)
  println(s"accuracy $accuracy")

  // more metrics with label, prediction
  val predictionDf = prDf.select("label", "prediction")
  println(s"total ${predictionDf.count()}")

  // correct
  val correct = predictionDf.filter($"label" === $"prediction").count()
  println($"correct $correct")

  // wrong
  val wrong = predictionDf.filter(not($"label" === $"prediction")).count()
  println($"wrong $wrong")

  // true positive
  // how often model correctly predict tumour was malignant
  val trueP = predictionDf.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
  println($"true positive $trueP")

  // true negative
  // how often model correctly predict tumour was benign
  val trueN = predictionDf.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
  println($"true negative $trueN")

  // false positive
  // how often model predict tumour was malignant when tumour was not malignant
  val falseP = predictionDf.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
  println($"false positive $falseP")

  // false negative
  // how often model predict tumour was benign when tumour was not benign
  val falseN = predictionDf.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
  println($"false positive $falseN")

  // correct ratio
  val ratioCorrect = correct / predictionDf.count().toDouble
  println(s"correct ratio $ratioCorrect")

  // wrong ratio
  val ratioWrong = wrong / predictionDf.count().toDouble
  println(s"wrong ratio $ratioWrong")

}

