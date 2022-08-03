package com.score.aplos

import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DecisionTreeLab extends App {

  case class Flight(_id: String, dofW: Int, carrier: String, origin: String, dest: String, crsdephour: Int, crsdeptime: Double,
                    depdelay: Double, crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dis: Double)

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema of uber
  val schema = StructType(
    StructField("_id", StringType, nullable = true) ::
      StructField("dofW", IntegerType, nullable = true) ::
      StructField("carrier", StringType, nullable = true) ::
      StructField("origin", StringType, nullable = true) ::
      StructField("dest", StringType, nullable = true) ::
      StructField("crsdephour", IntegerType, nullable = true) ::
      StructField("crsdeptime", DoubleType, nullable = true) ::
      StructField("depdelay", DoubleType, nullable = true) ::
      StructField("crsarrtime", DoubleType, nullable = true) ::
      StructField("arrdelay", DoubleType, nullable = true) ::
      StructField("crselapsedtime", DoubleType, nullable = true) ::
      StructField("dis", DoubleType, nullable = true) ::
      Nil
  )

  // read flight.json file to DataFrame
  val flightDf = spark.read
    .schema(schema)
    .json(getClass.getResource("/flight.json").getPath)
    .as[Flight]
    .cache()
  flightDf.show(10)
  flightDf.printSchema()

  // summary
  flightDf.describe("dis", "crselapsedtime", "arrdelay", "depdelay").show()

  // 5 longest depature delays
  flightDf.select("carrier", "origin", "dest", "depdelay", "arrdelay")
    .sort($"depdelay".desc)
    .show(5)

  // average depature delay by carrier
  flightDf.groupBy("carrier").avg("depdelay").show()

  // count depature delay by carrier (where deplay > 40)
  flightDf
    .filter("depdelay > 40")
    .groupBy("carrier").count()
    .show()

  // count of depature delay by day off week (where deplay > 40)
  flightDf
    .filter("depdelay > 40")
    .groupBy("dofW").count()
    .show()

  // count of depature delay by hour of day (where deplay > 40)
  flightDf
    .filter("depdelay > 40")
    .groupBy("crsdephour").count()
    .show()

  // count of depature order(delay and origin)
  flightDf.select("origin", "dest", "depdelay")
    .filter("depdelay > 40")
    .groupBy("origin", "dest")
    .count()
    .show()

  // label of delayed flights and count with bucketizer
  val bucketizer = new Bucketizer().setInputCol("depdelay")
    .setOutputCol("delayed")
    .setSplits(Array(0.0, 40.0, Double.PositiveInfinity))
  val bucketizeDf = bucketizer.transform(flightDf)
  bucketizeDf.groupBy("delayed").count().show()

  val fractions = Map(0.0 -> .29, 1.0 -> 1.0)
  val strain = bucketizeDf.stat.sampleBy("delayed", fractions, 36L)
  strain.groupBy("delayed").count.show()

  val columns = Array("carrier", "origin", "dest", "dofW")
  val stringIndexer = columns.map {c =>
    new StringIndexer()
      .setInputCol(c)
      .setOutputCol(c + "Indexed")
  }

  val encoders = columns.map {c =>
    new OneHotEncoder()
      .setInputCol(c + "Indexed")
      .setOutputCol(c + "Enc")
  }

  val labeler = new Bucketizer().setInputCol("depdelay")
    .setOutputCol("label")
    .setSplits(Array(0.0, 40.0, Double.PositiveInfinity))
  val featureCols = Array("carrierEnc", "destEnc", "originEnc", "dofWEnd", "crsdephour", "crselapsedtime", "crsarrtime", "crsdeptime", "dist")
  val assember = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

}
