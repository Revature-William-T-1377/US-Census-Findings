package etl //etl package

import org.apache.spark.sql.DataFrame
import sparkConnector.run.session

import scala.language.postfixOps
import scala.sys.process._

object andyFixedETL {
  var com1: DataFrame = _
  var com2: DataFrame = _
  var com3: DataFrame = _
  def ETLfunction(): Unit = {
    val t1 = System.nanoTime
//    val spark = SparkSession
//      .builder
//      .appName("hello hive")
//      .config("spark.master", "local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    println("Created spark session.")

    val statelist = List("ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
      "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or",
      "pa", "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy")

    //initialize headers
    val dfC1 = session.spark.read.format("csv").option("header", "true").load("datasets/2000/al00001.csv")
    dfC1.createOrReplaceTempView("Df1Imp")
    var dfCL1 = session.spark.sql("SELECT * FROM Df1Imp LIMIT 1")

    val dfC2 = session.spark.read.format("csv").option("header", "true").load("datasets/2010/al00001.csv")
    dfC2.createOrReplaceTempView("Df2Imp")
    var dfCL2 = session.spark.sql("SELECT * FROM Df2Imp LIMIT 1")

    val dfC3 = session.spark.read.format("csv").option("header", "true").load("datasets/2020/al00001.csv")
    dfC3.createOrReplaceTempView("Df3Imp")
    var dfCL3 = session.spark.sql("SELECT * FROM Df3Imp LIMIT 1")

    //loads first line of each state to get total population in regards to a state as well as group values
    statelist.foreach(i => {

      val dfC1 = session.spark.read.format("csv").option("header", "true").load(s"datasets/2000/${i}00001.csv")
      dfC1.createOrReplaceTempView("Df1Imp")

      val dfCL1M = session.spark.sql("SELECT * FROM Df1Imp LIMIT 1")

      val com1 = dfCL1.union(dfCL1M).distinct()
      dfCL1 = com1
    })

    statelist.foreach(i => {

      val dfC2 = session.spark.read.format("csv").option("header", "true").load(s"datasets/2010/${i}00001.csv")
      dfC2.createOrReplaceTempView("Df2Imp")

      val dfCL2M = session.spark.sql("SELECT * FROM Df2Imp LIMIT 1")

      val com2 = dfCL2.union(dfCL2M).distinct()
      dfCL2 = com2
    })

    statelist.foreach(i => {

      val dfC3 = session.spark.read.format("csv").option("header", "true").load(s"datasets/2020/${i}00001.csv")
      dfC3.createOrReplaceTempView("Df3Imp")

      val dfCL3M = session.spark.sql("SELECT * FROM Df3Imp LIMIT 1")

      val com3 = dfCL3.union(dfCL3M).distinct()
      dfCL3 = com3
    })

    dfCL1.createOrReplaceTempView("preRegion1")
    dfCL2.createOrReplaceTempView("preRegion2")
    dfCL3.createOrReplaceTempView("preRegion3")

    //uses regions divisions csv file to add regions to each state
    val regions = session.spark.read.option("header", "true").csv("tableFiles/RegionsDivisions.csv")
    regions.createOrReplaceTempView("regionsTemp")

    dfCL1 = session.spark.sql("SELECT * FROM regionsTemp FULL JOIN preRegion1 ON `State Code` = preRegion1.STUSAB")
    dfCL1 = dfCL1.drop("FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO")
    dfCL1 = dfCL1.withColumnRenamed("State Code", "STUSAB")

    dfCL2 = session.spark.sql("SELECT * FROM regionsTemp FULL JOIN preRegion2 ON `State Code` = preRegion2.STUSAB")
    dfCL2 = dfCL2.drop("FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO")
    dfCL2 = dfCL2.withColumnRenamed("State Code", "STUSAB")

    dfCL3 = session.spark.sql("SELECT * FROM regionsTemp FULL JOIN preRegion3 ON `State Code` = preRegion3.STUSAB")
    dfCL3 = dfCL3.drop("FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO")
    dfCL3 = dfCL3.withColumnRenamed("State Code", "STUSAB")

//    dfCL1.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputCsv/2000/")
//    dfCL2.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputCsv/2010/")
//    dfCL3.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputCsv/2020/")

    //outputs file
    file.outputcsv("Combine2000RG",dfCL1)
    file.outputcsv("Combine2010RG",dfCL2)
    file.outputcsv("Combine2020RG",dfCL3)


    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration / 1000000000) + " Seconds")
  }

//  def main(args: Array[String]): Unit = {
//    ETLfunction()
//  }
}
