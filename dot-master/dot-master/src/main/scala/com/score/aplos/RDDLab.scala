package com.score.aplos

import org.apache.spark.{SparkConf, SparkContext}

object RDDLab extends App {
  // spark config
  val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("lambda")
  val context = new SparkContext(conf)

  case class Row(year: String, name: String, country: String, sex: String, count: Int)

  // lambda csv file
  val lambdaText = context.textFile(getClass.getResource("/lambda.csv").getPath)

  // filter out csv header and convert rows to lambdas
  // cache rows in memory since we are reusing it several times, otherwise it will recalculate
  // all the time, note that cache required more memory
  val rows = lambdaText.filter(p => !p.contains("Year"))
    .map(p => p.split(","))
    .map(p => Row(p(0), p(1), p(2), p(3), p(4).toInt))
    .cache()

  // unique names
  // take first 10 for print
  rows.map(p => p.name)
    .distinct()
    .sortBy(p => p)
    .take(10)
    .foreach(println)

  // total occurrences of luke name
  val d1 = rows.map(p => p.name)
    .filter(_.contains("LUKE"))
    .count()
  println(d1)

  // total occurrences of different names
  // take first 10 for print
  rows.map(p => p.name -> 1)
    .reduceByKey((v1, v2) => v1 + v2)
    .take(10)
    .foreach(println)

  // total occurrences of different names
  // sort name with ascending order
  // take first 10 for print
  rows.map(p => p.name -> 1)
    .reduceByKey((v1, v2) => v1 + v2)
    .sortBy(p => p._1)
    .take(10)
    .foreach(println)

  // names which occur more than 100
  rows.map(p => p.name -> 1)
    .reduceByKey(_ + _)
    .filter(_._2 > 400)
    .foreach(println)

  // top 10 occurring names
  // sort occurrence with descending order
  rows.map(p => p.name -> p.count)
    .reduceByKey((v1, v2) => v1 + v2)
    .sortBy(_._2, ascending = false)
    .take(10)
    .foreach(println)

  // total count of luke name
  rows.filter(p => p.name.contains("LUKE"))
    .map(p => p.name -> p.count)
    .reduceByKey((v1, v2) => v1 + v2)
    .foreach(println)

  // top 10 luke name count
  rows.filter(p => p.name.contains("LUKE"))
    .sortBy(_.count, ascending = false)
    .take(10)
    .foreach(println)

  // top 10 baby born years
  rows.map(p => p.year -> p.count)
    .reduceByKey((v1, v2) => v1 + v2)
    .sortBy(_._2, ascending = false)
    .take(10)
    .foreach(println)

  // top 10 male baby born years
  rows.filter(p => p.sex.equalsIgnoreCase("M"))
    .map(p => p.year -> p.count)
    .reduceByKey((v1, v2) => v1 + v2)
    .sortBy(_._2, ascending = false)
    .take(10)
    .foreach(println)

  // male count in each year
  rows.filter(p => p.sex.equalsIgnoreCase("M"))
    .map(p => p.year -> p.count)
    .reduceByKey((v1, v2) => v1 + v2)
    .foreach(println)

}

