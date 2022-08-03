package com.score.aplos

import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._

object CassandraLab extends App {

  case class Document(id: String, approved: Boolean, blob: String, creator: String, dept: String,
                      signatures: List[String], signers: List[String], timestamp: Date, typ: String)

  // context for spark
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("lambda")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  // SparkSession has implicits
  import spark.implicits._

  // documents RDD
  val documentRDD = spark.sparkContext
    .cassandraTable[Document]("mystiko", "documents")
    .select("id", "approved", "blob", "creator", "dept", "signatures", "signers", "timestamp", "typ")

  // view data
  documentRDD.take(10).foreach(println)

  // filter
  documentRDD.filter(_.signatures.contains("eranga")).take(10).foreach(println)

  // documents DF
  val documentDF = spark.read
    .cassandraFormat("documents", "mystiko")
    .options(ReadConf.SplitSizeInMBParam.option(32))
    .load()

  // view schema
  documentDF.printSchema()

  // view data
  documentDF.show(10)

  // filter, approved = false AND dept = dev AND signers contains 'eranga'
  documentDF.filter($"approved" === false)
    .filter($"dept" === "dev")
    .filter(array_contains($"signers", "eranga"))
    .show(10)

}
