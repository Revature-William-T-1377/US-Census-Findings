package com.score.aplos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataFrameLab extends App {

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // schema of uber
  val schema = StructType(
    StructField("Time", TimestampType, nullable = true) ::
      StructField("Lat", DoubleType, nullable = true) ::
      StructField("Lon", DoubleType, nullable = true) ::
      StructField("Base", StringType, nullable = true) ::
      Nil
  )

  // read user.csv file to DataFrame
  val uberDf = spark.read.format("csv")
    .option("header", value = true)
    .option("delimiter", ",")
    .option("mode", "DROPMALFORMED")
    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
    .schema(schema)
    .load(getClass.getResource("/uber.csv").getPath)
    .cache()
  uberDf.show(10)
  uberDf.printSchema()

  val deviceDf = Seq(
    ("device1", "event1", 10, "2016-05-01 10:50:51"),
    ("device2", "event2", 100, "2016-05-01 10:50:53"),
    ("device1", "event3", 20, "2016-05-01 10:50:55"),
    ("device1", "event1", 15, "2016-05-01 10:51:50"),
    ("device3", "event1", 13, "2016-05-01 10:55:30"),
    ("device1", "event2", 12, "2016-05-01 10:57:00"),
    ("device1", "event3", 11, "2016-05-01 11:00:01")
  ).toDF("Device", "Event", "Metric", "Time").cache()

  // schema of DataFrame
  deviceDf.printSchema()

  // view first 5 rows
  deviceDf.show(5)

  // summary statistics on selected columns
  deviceDf.describe("metric", "time").show()

  // select Device, Event and Time columns
  deviceDf.select("Device", "Event", "Time").show(5)

  // filter Device = device1 AND Metrics > 11
  deviceDf.filter($"Device" === "device1" && $"Metric" > 11).show()

  // filter Device = device1 AND Event in (event1, event2)
  deviceDf.filter($"Device" === "device1")
    .filter("Metric in (11, 12, 13)").show()

  // sort the records with Event field, Ascending order
  deviceDf.sort($"Event".asc).show()

  // sort the records with Event field, Descending order
  deviceDf.sort($"Event".desc).show()

  // distinct devices
  deviceDf.select("Device").distinct().show()

  // distinct devices and events
  deviceDf.select("Device", "Event").distinct().show()

  // group similar devices and add new count column
  deviceDf.groupBy("Device").count().show()

  // group similar devices and sum Metric
  deviceDf.groupBy("Device").avg("Metric").show()

  // group with time interval
  deviceDf.groupBy($"Device", window($"Time", "5 minutes").alias("TimeWindow"))
    .count()
    .show()


  // data frame with arrayType
  val documentDf1 = List(
    ("0001", "invoice", List("eranga", "isuru", "asitha"), "2016-05-01 10:50:51"),
    ("0002", "order", List("eranga", "asitha"), "2016-05-01 10:50:55"),
    ("0003", "payment", List("eranga", "kasun", "asitha"), "2016-05-03 8:50:55"),
    ("0003", "payment", List("kasun"), "2016-05-03 8:53:35")
  ).toDF("id", "type", "signers", "time").cache()

  // schema
  documentDf1.printSchema()

  // view data
  documentDf1.show()

  // filter type = 'payment' AND signers contains 'eranga'
  documentDf1.filter($"type" === "payment")
    .filter(array_contains($"signers", "eranga"))
    .show()

  case class Document(id: String, typ: String, signers: List[Signer], time: String)

  case class Signer(name: String, email: String)

  // data frame with arrayType
  val documentDf2 = List(
    Document("0001", "invoice", List(Signer("eranga", "e@gmai.com"), Signer("isuru", "i@g.com"), Signer("asitha", "a@g.com")), "2016-05-01 10:50:51"),
    Document("0002", "order", List(Signer("eranga", "e@gmai.com"), Signer("asitha", "a@g.com")), "2016-05-01 10:50:55"),
    Document("0003", "payment", List(Signer("eranga", "e@gmai.com"), Signer("kasun", "k@g.com"), Signer("asitha", "a@g.com")), "2016-05-03 8:50:55"),
    Document("0003", "payment", List(Signer("kasun", "k@g.com")), "2016-05-03 8:53:35")
  ).toDF("id", "type", "signers", "time").cache()

  // schema
  documentDf2.printSchema()

  // view data
  documentDf2.show()

  // filter type = 'payment' AND signers contains 'eranga'
  documentDf2.filter($"typ" === "payment")
    .filter(array_contains($"signers.name", "eranga"))
    .show()
}
