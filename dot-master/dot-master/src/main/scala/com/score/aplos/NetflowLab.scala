package com.score.aplos

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object NetflowLab extends App {

  case class Packet(uid: String, srcMac: String, dstMac: String, srcAddr: String, srcPort: String, inSnmp: String, dstAddr: String,
                    dstPort: String, outSnmp: String, protocol: String)

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("netflow")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema of uber
  val schema = StructType(
    StructField("uid", StringType, nullable = true) ::
      StructField("srcMac", StringType, nullable = true) ::
      StructField("dstMac", StringType, nullable = true) ::
      StructField("srcAddr", StringType, nullable = true) ::
      StructField("srcPort", StringType, nullable = true) ::
      StructField("inSnmp", StringType, nullable = true) ::
      StructField("dstAddr", StringType, nullable = true) ::
      StructField("dstPort", StringType, nullable = true) ::
      StructField("outSnmp", StringType, nullable = true) ::
      StructField("protocol", StringType, nullable = true) ::
      Nil
  )

  // read netflow.json file to DataFrame
  val netflowDf = spark.read
    .schema(schema)
    .json(getClass.getResource("/netflow.json").getPath)
    .as[Packet]
    .cache()
  netflowDf.show(10)
  netflowDf.printSchema()

  val df = netflowDf
    .withColumn("srcPort", $"srcPort" cast "Int")
    .withColumn("dstPort", $"dstPort" cast "Int")
    .withColumn("inSnmp", $"inSnmp" cast "Int")
    .withColumn("outSnmp", $"outSnmp" cast "Int")
  df.show(10)
  df.printSchema()

  val cols = List("srcAddr", "dstAddr")
  val indexers = cols.map {col =>
    new StringIndexer()
      .setInputCol(col)
      .setOutputCol(s"${col}_indexed")
  }
  val pipeline = new Pipeline().setStages(indexers.toArray)
  val dataFrame3 = pipeline.fit(df).transform(df)

//  val indexer = new StringIndexer()
//    .setInputCol("srcAddr")
//    .setOutputCol("address")
//  val dataFrame3 = indexer.fit(df).transform(df).drop("srcAddr")
  dataFrame3.show()

  netflowDf.filter($"srcAddr" === "10.254.8.75")
    .show()

  netflowDf.select("srcAddr").distinct().show()

  netflowDf.groupBy("srcAddr").count().show()

  netflowDf.groupBy("srcAddr").count().sort($"count".desc).show()

}
