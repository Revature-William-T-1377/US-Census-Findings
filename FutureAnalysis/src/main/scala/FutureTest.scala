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
    def projection(stateCode: String, year1: Long, year2: Long, year3: Long): Array[Array[Long]]= {
      var years = Array(
        Array(2000, year1),
        Array(2010, year2),
        Array(2020, year3)
      )

      for(i <- 1 to 3) {
        var year1 :Double  = years(years.size - 3)(1)
        var year2 :Double = years(years.size - 2)(1)
        var year3 :Double = years(years.size - 1)(1)
        var growth1 = ((year2 - year1 )/ year1)

        println(stateCode + " population in 2000 : "+ year1 )
        println(stateCode + " population in 2010 : "+ year2 )
        println(stateCode + " population in 2020 : "+ year3 )
        println("Growth 1 is  : "+ growth1)
        var growth2 = ((year3 - year2) / year2)
        println("Growth 2 is  : "+ growth2)
        var derivative = (growth1 - growth2)  // negative downtrends :3c
        println("Derivation is : " + derivative)
        var growthDecay = 1- (derivative / growth1)
        var year = 2020 + (i * 10)
        println("Predicted Year is :" +year)
        var population =(years.last(1) * (1 + (growth2 * growthDecay))).toLong
        println("Future Population of " + stateCode + " in " + year+ "  :" + population)
        println()

        years = years :+ Array(year, population)
      }
      years
    }
    var data = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(
      "Cleaned/Combine2000RG.csv")

    var data2010 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(
      "Cleaned/Combine2010RG.csv")
    var data2020 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(
      "Cleaned/combine2020RG.csv")

    data.createOrReplaceTempView("Census2000")
    data2010.createOrReplaceTempView("Census2010")
    data2020.createOrReplaceTempView("Census2020")

    var df =spark.sql(" SELECT DISTINCT(f.STUSAB) , s.P0010001  , f.P0010001  , t.P0010001" +
      " FROM Census2000  f INNER JOIN Census2010 s ON f.STUSAB =s.STUSAB INNER JOIN Census2020 t ON s.STUSAB=t.STUSAB Order By f.STUSAB  ").toDF("STUSAB", "Pop2010", "Pop2000", "Pop2020")
    df.show(53)
    var list2010=df.select("Pop2010").collectAsList()
    var list2000=df.select("Pop2000").collectAsList()
    var list2020=df.select("Pop2020").collectAsList()
    var statecode=df.select("STUSAB").collectAsList()


      for(i <- 0 to list2020.length-1) {
        println(projection(statecode(i)(0).toString, list2000(i)(0).toString.toLong, list2010(i)(0).toString.toLong, list2020(i)(0).toString.toLong))
    }


  }
}
