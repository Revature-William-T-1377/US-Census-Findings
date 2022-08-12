import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scala.language.postfixOps
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object newcities {
  var t1 = System.nanoTime
  var startgeo: DataFrame = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")

    var low2000 = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\Work\\data\\testing\\Geolow2000\\Geolow2000.csv") // CHANGE
    low2000.createOrReplaceTempView("low2000imp")

    var low2020 = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\Work\\data\\testing\\Geolow\\Geolow2020.csv")
    low2020.createOrReplaceTempView("low2020imp")

    var high2000 = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\Work\\data\\testing\\Geohigh2000\\Geohigh2000.csv")
    high2000.createOrReplaceTempView("high2000imp")

    var high2020 = spark.read.format("csv").option("header", "true").load("D:\\Downloads\\Work\\data\\testing\\Geohigh\\Geohigh2020.csv")
    high2020.createOrReplaceTempView("high2020imp")

    //-----------------------------------------------------Queries----------------------------------------------------

    println("1")
    var newover100000M = spark.sql("SELECT * FROM high2020imp a ANTI JOIN high2000imp b ON a.NAME = b.NAME " +
      "WHERE NOT (a.NAME LIKE '%balance%') AND NOT (a.NAME LIKE '%county%');")
    newover100000M.createOrReplaceTempView("newover100000M")
    newover100000M.show(100)

    println("2")
    var joinover = spark.sql("SELECT a.NAME, FIRST(a.INTPTLAT), FIRST(a.INTPTLON), SUM(a.POP100)-SUM(b.POP100) " +
      "AS POPDIFF FROM newover100000M a INNER JOIN low2000imp b ON a.NAME = b.NAME GROUP BY a.NAME")
    joinover.show(100)

    println("3")
    var newunder100000M = spark.sql("SELECT * FROM high2000imp a ANTI JOIN high2020imp b ON a.NAME = b.NAME " +
      "WHERE NOT (a.NAME LIKE '%balance%') AND NOT (a.NAME LIKE '%county%');")
    newunder100000M.createOrReplaceTempView("newunder100000M")
    newunder100000M.show(100)

    println("4")
    var joinunder = spark.sql("SELECT a.NAME, FIRST(a.INTPTLAT), FIRST(a.INTPTLON), SUM(b.POP100)-SUM(a.POP100) " +
      "AS POPDIFF FROM newunder100000M a INNER JOIN low2020imp b ON a.NAME = b.NAME GROUP BY a.NAME")
    joinunder.show(100)

   var jointotal = joinover.union(joinunder).distinct()

    //-----------------------------------------------------Save----------------------------------------------------

    jointotal.coalesce(1).write.option("header", "true").csv("D:\\Downloads\\Work\\data\\testing\\Geodiffs\\totaldiffs")

    var duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration / 1000000000) + " Seconds")
  }
}