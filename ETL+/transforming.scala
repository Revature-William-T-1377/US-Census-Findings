import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.DataFrame

object transforming extends App{
  val t1 = System.nanoTime

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("Created spark session.")

  val statelist = List("ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
  "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or",
  "pa", "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy")

  var com1: DataFrame = _
  var com2: DataFrame = _
  var com3: DataFrame = _

  var dfC1 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs1\\al00001.csv")
  dfC1.createOrReplaceTempView("Df1Imp")
  var dfCL1 = spark.sql("SELECT * FROM Df1Imp LIMIT 1")

  var dfC2 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs2\\al000012010.csv")
  dfC2.createOrReplaceTempView("Df2Imp")
  var dfCL2 = spark.sql("SELECT * FROM Df2Imp LIMIT 1")

  var dfC3 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs3\\al00001.csv")
  dfC3.createOrReplaceTempView("Df3Imp")
  var dfCL3 = spark.sql("SELECT * FROM Df3Imp LIMIT 1")

  statelist.foreach( i => {

    var dfC1 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs1\\${i}00001.csv")
    dfC1.createOrReplaceTempView("Df1Imp")

    var dfCL1M = spark.sql("SELECT * FROM Df1Imp LIMIT 1")

    var com1 = dfCL1.union(dfCL1M).distinct()
    dfCL1 = com1
  })

  statelist.foreach( i => {

    var dfC2 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs2\\${i}000012010.csv")
    dfC2.createOrReplaceTempView("Df2Imp")

    var dfCL2M = spark.sql("SELECT * FROM Df2Imp LIMIT 1")

    var com2 = dfCL2.union(dfCL2M).distinct()
    dfCL2 = com2
  })

  statelist.foreach( i => {

    var dfC3 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs\\CSVs3\\${i}00001.csv")
    dfC3.createOrReplaceTempView("Df3Imp")

    var dfCL3M = spark.sql("SELECT * FROM Df3Imp LIMIT 1")

    var com3 = dfCL3.union(dfCL3M).distinct()
    dfCL3 = com3
  })

  dfCL1.coalesce(1).write.option("header", "true").csv("D:\\Revature\\DowloadDataScala\\FinalExport\\OutputCSV2\\2000")
  dfCL2.coalesce(1).write.option("header", "true").csv("D:\\Revature\\DowloadDataScala\\FinalExport\\OutputCSV2\\2010")
  dfCL3.coalesce(1).write.option("header", "true").csv("D:\\Revature\\DowloadDataScala\\FinalExport\\OutputCSV2\\2020")

  val duration = (System.nanoTime - t1)
  println("Code Lasted: " + (duration/1000000000) + " Seconds")
}
