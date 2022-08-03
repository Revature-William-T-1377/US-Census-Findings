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

  //println("df1")
  //val df1 = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\CSVs\\co00001.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  //df1.createOrReplaceTempView("Co1Imp")

//  df1.write.mode("overwrite").saveAsTable("Co1")
//  spark.sql("SELECT * FROM Co1").show()

  //println("df2")
  //val df2 = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\CSVs\\co00002.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  //df2.createOrReplaceTempView("Co2Imp")

  //println("df3")
  //val df3 = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\CSVs\\ak00001.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  //df3.createOrReplaceTempView("Ak1Imp")

//  df2.write.mode("overwrite").saveAsTable("Co2")
//  spark.sql("SELECT * FROM Co2").show()

// ------------------------------------------------------Modifications--------------------------------------------------------------------------------
/*
  println("df2M")
  var df2M = df2.drop("chariter", "cifsn", "fileid", "stusab", "LOGRECNO")
  //df2M = df2M.withColumnRenamed("LOGRECNO","LOGRECNO2")

  var df1M = df1.withColumn("id1", monotonically_increasing_id)  // Makes id column to join
  df2M = df2M.withColumn("id2", monotonically_increasing_id)    // Then it will be dropped

  df1M.createOrReplaceTempView("Co1ImpM")
  df1M.show()

  df2M.createOrReplaceTempView("Co2ImpM")
  df2M.show()

  var export1 = df1M.join(df2M,col("id1")===col("id2"),"inner") //Joines with id column then drops it.
    .drop("id1","id2")

  file.outputcsv("CoCombine1",export1)
  export1.show()
*/
  val statelist = List("ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky",
  "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or",
  "pa", "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy")

  var com1: DataFrame = _

  println("dfC1")
  var dfC1 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs3\\al00001.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  dfC1.createOrReplaceTempView("Df1Imp")
  var dfCL1 = spark.sql("SELECT * FROM Df1Imp LIMIT 1")

  statelist.foreach( i => { println(i)

    //  df1.write.mode("overwrite").saveAsTable("Co1")
    //  spark.sql("SELECT * FROM Co1").show()

    println("dfC2")
    var dfC2 = spark.read.format("csv").option("header", "true").load(s"D:\\Revature\\DowloadDataScala\\CSVs3\\${i}00001.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
    dfC2.createOrReplaceTempView("Df2Imp")

    var dfCL2 = spark.sql("SELECT * FROM Df2Imp LIMIT 1")

    var com1 = dfCL1.union(dfCL2).distinct() //unionByName().distinct()
    dfCL1 = com1
    //dfCL1.show()
  })

  file.outputcsv("Combine2020", dfCL1)

  spark.sql("SHOW DATABASES").show()
  spark.sql("SHOW TABLES").show()

  val duration = (System.nanoTime - t1)
  println("Code Lasted: " + (duration/1000000000) + " Seconds")
}
