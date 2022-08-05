import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.convert.ImplicitConversions.{`list asScalaBuffer`, `map AsJavaMap`}


object FutureTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FutureTest")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val data2000 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource("/Combine2000RG.csv").getPath)

    val data2010 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource(
      "/Combine2010RG.csv").getPath)
    val data2020 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource(
      "/combine2020RG.csv").getPath)

    data2000.createOrReplaceTempView("Census2000")
    data2010.createOrReplaceTempView("Census2010")
    data2020.createOrReplaceTempView("Census2020")

    val df =spark.sql(" SELECT DISTINCT(f.STUSAB) , f.P0010001  , s.P0010001  ,  t.P0010001" +
      " FROM Census2000  f INNER JOIN Census2010 s ON f.STUSAB =s.STUSAB INNER JOIN Census2020 t ON s.STUSAB=t.STUSAB Order By f.STUSAB  ").toDF("STUSAB", "Pop2000", "Pop2010", "Pop2020")
    df.show(53)
    val statecode=df.select("STUSAB").collectAsList()
    val list2000=df.select("Pop2000").collectAsList()
    val list2010=df.select("Pop2010").collectAsList()
    val list2020=df.select("Pop2020").collectAsList()


    for(i <- 0 to statecode.length-1) {

      projection(statecode(i)(0).toString, list2000(i)(0).toString.toLong, list2010(i)(0).toString.toLong, list2020(i)(0).toString.toLong)
    }
  }

  def projection(stateCode: String, year1: Long, year2: Long, year3: Long): Unit= {
    var years = Array(
      Array(2000, year1),
      Array(2010, year2),
      Array(2020, year3)
    )

    for(i <- 1 to 3) {
      val year1 :Double  = years(years.size - 3)(1)
      val year2 :Double = years(years.size - 2)(1)
      val year3 :Double = years(years.size - 1)(1)
      val growth1 = ((year2 - year1 )/ year1)

      println(stateCode + s" population in ${years(years.size - 3)(0)}: "+ year1 )
      println(stateCode + s" population in ${years(years.size - 2)(0)}: "+ year2 )
      println(stateCode + s" population in ${years(years.size - 1)(0)}:  "+ year3 )
      println("Growth 1 is : "+ growth1)
      val growth2 = ((year3 - year2) / year2)
      println("Growth 2 is : "+ growth2)
      val derivative = (growth1 - growth2)  // negative downtrends :3c
      println("Derivation is: " + derivative)
      val growthDecay = 1- (derivative / growth1)
      val year = 2020 + (i * 10)
      println("Predicted Year is:" +year)
      val population =(years.last(1) * (1 + (growth2 * growthDecay))).toLong
      println("Future Population of " + stateCode + " in " + year+ ": " + population)
      println()

      years = years :+ Array(year, population)
    }
  }
}
