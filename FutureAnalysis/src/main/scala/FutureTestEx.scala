
 import org.apache.log4j.{Level, Logger}
 import org.apache.spark.sql.{DataFrame, SparkSession}

 import java.io.{File, FileReader, FileWriter}
 import scala.collection.convert.ImplicitConversions.{`list asScalaBuffer`, `map AsJavaMap`}

object FutureTestEx {
  val fileName = "FuturePredict.csv"  //Set Export CSV filename 
  val mymultiarr= Array.ofDim[String](1, 7) //Create Array with State code name

  def ExportCSV: Unit ={  //Function for export CSv
      val ColumnSeparator = ","  //separate by comma for export csv
      val writer = new FileWriter(fileName, true)

    try {

        mymultiarr.foreach{
          line =>
            writer.write(s"${line.map(_.toString).mkString(ColumnSeparator)}\n") // Appending each array until loop end
        }
      } finally {
        writer.flush()
        writer.close() //Close CSV Writer
      }
}
  def deleteFile(path: String) = {  //Check if file existing
    val fileTemp = new File(path)
    if (fileTemp.exists) {
      fileTemp.delete()
    }
  }

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
      println(stateCode + " population  : "+ year1 )
      println(stateCode + " population  : "+ year2 )
      println(stateCode + " population  : "+ year3 )
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
      println() //Extra line
      years = years :+ Array(year, population)
      mymultiarr(0)(i+3) = population.toString  //Adding Predicted population in FOR loop for 2030,2040,2050

    }


    mymultiarr(0)(0) = stateCode //First index is States Code
    mymultiarr(0)(1) = year1.toString //2nd index default 2000 population
    mymultiarr(0)(2) = year2.toString //3rd index default 2010 population
    mymultiarr(0)(3) = year3.toString //4th index default 2020 population
    ExportCSV  //Function Called to export these outputs as CSV files

    years //Return Year Array
  }
  def main(args: Array[String]): Unit = {
    deleteFile(fileName)
    val spark = SparkSession
      .builder
      .appName("FutureTest")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val data = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource("/Combine2000RG.csv").getPath)

    val data2010 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource(
      "/Combine2010RG.csv").getPath)
    val data2020 = spark.read.option("header", "true").option("inferSchema",
      "true").format("csv").load(getClass.getResource(
      "/combine2020RG.csv").getPath)
//Creating Temp View
    data.createOrReplaceTempView("Census2000")
    data2010.createOrReplaceTempView("Census2010")
    data2020.createOrReplaceTempView("Census2020")
//Join 3 Decade Population  table data
    var df =spark.sql(" SELECT DISTINCT(f.STUSAB) , f.P0010001  , s.P0010001  , t.P0010001" +
      " FROM Census2000  f INNER JOIN Census2010 s ON f.STUSAB =s.STUSAB INNER JOIN Census2020 t ON s.STUSAB=t.STUSAB Order By f.STUSAB  ").toDF("STUSAB", "Pop2000", "Pop2010", "Pop2020")
    df.show(53)
    var list2010=df.select("Pop2010").collectAsList()
    var list2000=df.select("Pop2000").collectAsList()
    var list2020=df.select("Pop2020").collectAsList()
    var statecode=df.select("STUSAB").collectAsList()

      for(i <- 0 to list2020.length-1) {
        //passing parameter to prediction function using loop, This example passing value of Column name "P0010001"
        projection(statecode(i)(0).toString, list2000(i)(0).toString.toLong, list2010(i)(0).toString.toLong, list2020(i)(0).toString.toLong)
      }
  }
}
