import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, first, lit, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import java.io.{File, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileReader, FileWriter}
import scala.collection.convert.ImplicitConversions.{`list asScalaBuffer`, `map AsJavaMap`}

object Main {
  val spark = new sparkAWS

  val mymultiarr= Array.ofDim[String](1, 7) //Create Array with State code name

  def AddHeader(fileName: String): Unit ={
    val writer = new FileWriter(fileName, true)
    try {
      writer.append("StateID,2000 Decade,2010 Decade,2020 Decade,2030 Decade,2040 Decade,2050 Decade\n") // Appending each array until loop end
    } finally {
      writer.flush()
      writer.close() //Close CSV Writer
    }
  }

  def ExportCSV(file: String) : Unit ={  //Function for export CSv
    val ColumnSeparator = ","  //separate by comma for export csv
    val writer = new FileWriter(file, true)

    try {
      mymultiarr.foreach{
        line =>
          writer.append(s"${line.map(_.toString).mkString(ColumnSeparator)}\n") // Appending each array until loop end
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

  def decayProjection(stateCode: String, year1: Long, year2: Long, year3: Long, fileName: String) {
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

      var growth2 = ((year3 - year2) / year2)
      var derivative = (growth1 - growth2)  // negative downtrends :3c
      var growthDecay = 1- (derivative / growth1)
      var year = 2020 + (i * 10)
      var population =(years.last(1) * (1 + (growth2 * growthDecay))).toLong
      years = years :+ Array(year, population)
      mymultiarr(0)(i+3) = population.toString  //Adding Predicted population in FOR loop for 2030,2040,2050

    }
    mymultiarr(0)(0) = stateCode //First index is States Code
    mymultiarr(0)(1) = year1.toString //2nd index default 2000 population
    mymultiarr(0)(2) = year2.toString //3rd index default 2010 population
    mymultiarr(0)(3) = year3.toString //4th index default 2020 population
    ExportCSV(fileName)  //Function Called to export these outputs as CSV files


  }
  def slopeProjection(stateCode: String, year1: Long, year2: Long, year3: Long, fileNameSlope: String) {
    var years = Array(
      Array(2000, year1),
      Array(2010, year2),
      Array(2020, year3)
    )
    for(i <- 1 to 3) {
      var year1 :Double  = years(years.size - 3)(1)
      var year2 :Double = years(years.size - 2)(1)
      var year3 :Double = years(years.size - 1)(1)
      var growth1 = (year2 - year1)
      var growth2 = (year3 - year2)
      var growth3 = ((growth1 + growth2)/2)
      var year = 2020 + (i * 10)
      var population =(years.last(1) +growth3).toLong
      years = years :+ Array(year, population)
      mymultiarr(0)(i+3) = population.toString  //Adding Predicted population in FOR loop for 2030,2040,2050

    }
    mymultiarr(0)(0) = stateCode //First index is States Code
    mymultiarr(0)(1) = year1.toString //2nd index default 2000 population
    mymultiarr(0)(2) = year2.toString //3rd index default 2010 population
    mymultiarr(0)(3) = year3.toString //4th index default 2020 population
    ExportCSV(fileNameSlope)  //Function Called to export these outputs as CSV files


  }
  def decayHybridProjection(stateCode: String, year1: Long, year2: Long, year3: Long, fileNameHybrid: String) {
    var years = Array(
      Array(2000, year1),
      Array(2010, year2),
      Array(2020, year3)
    )
    var population = (years.last(1))
    for(i <- 1 to 3) {
      var year1 :Double  = years(years.size - 3)(1)
      var year2 :Double = years(years.size - 2)(1)
      var year3 :Double = years(years.size - 1)(1)
      var growth1 = ((year2 - year1 )/ year1)
      var growth2 = ((year3 - year2) / year2)
      var derivative = (growth1 - growth2)  // negative downtrends :3c
      // println(derivative)
      var growthDecay = 1- (derivative / growth1)
      if(derivative < 0) {
        // println("inside the if")
        growth1 = (year2 - year1)
        growth2 = (year3 - year2)
        var growth3 = ((growth1 + growth2)/2)
        var population =(years.last(1) + growth3).toLong
        var year = 2020 + (i * 10)
        years = years :+ Array(year, population)
        mymultiarr(0)(i+3) = population.toString  //Adding Predicted population in FOR loop for 2030,2040,2050
      }
      else
      {
        // println("inside the else")
        var population =(years.last(1) * (1 + (growth2 * growthDecay))).toLong
        var year = 2020 + (i * 10)
        years = years :+ Array(year, population)
        mymultiarr(0)(i+3) = population.toString  //Adding Predicted population in FOR loop for 2030,2040,2050
      }
    }
    mymultiarr(0)(0) = stateCode //First index is States Code
    mymultiarr(0)(1) = year1.toString //2nd index default 2000 population
    mymultiarr(0)(2) = year2.toString //3rd index default 2010 population
    mymultiarr(0)(3) = year3.toString //4th index default 2020 population
    ExportCSV(fileNameHybrid)  //Function Called to export these outputs as CSV files


  }

  def main(args: Array[String]): Unit = {
    //initializing spark session
    val session = spark

    //creating initial empty dataframes
    var df = session.spark.emptyDataFrame
    var df2 = session.spark.emptyDataFrame
    var df3 = session.spark.emptyDataFrame

//    if(args.length == 1) {
//      if (args(0) == "--cloud") {
//        //reading csvs from s3 bucket, turing into dataframe
//        val bucket = "revature-william-big-data-1377"
//        df = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2020RG.csv")//2020
//        df2 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2010RG.csv")//2010
//        df3 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2000RG.csv")//2000
//      } else {
//        println("command line argument not recognized")
//        System.exit(1)
//      }
//    } else if (args.length > 1){
//      println("too many command line arguments provided")
//      System.exit(1)
//    } else {
//      df = session.spark.read.option("header", "true").csv("hdfs://debsrv:9000/user/hive/warehouse/OutputCSV2/Combine2020RG.csv")
//      df2 = session.spark.read.option("header", "true").csv("hdfs://debsrv:9000/user/hive/warehouse/OutputCSV2/Combine2010RG.csv")
//      df3 = session.spark.read.option("header", "true").csv("hdfs://debsrv:9000/user/hive/warehouse/OutputCSV2/Combine2000RG.csv")
//    }


    val bucket = "revature-william-big-data-1377"
    df = session.spark.read.option("header", "true").csv(s"s3a://$bucket/OutputCSV2/Combine2020RG.csv") //2020
    df2 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/OutputCSV2/Combine2010RG.csv") //2010
    df3 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/OutputCSV2/Combine2000RG.csv") //2000

    //basic casting for dataframes
    df = df.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))

    //turns dataframes into tempviews for querying
    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")

    //running queries 1-6
    val query = queries.Queries
    session.spark.sql(query.query1()).show()
    session.spark.sql(query.query2()).show()
    session.spark.sql(query.query3()).show()
    val q31 = session.spark.sql(query.query31())
    val q32 = session.spark.sql(query.query32())
    val q3f = q32.union(q31)
    q3f.show()
    session.spark.sql(query.query4()).show()
    session.spark.sql(query.query5()).show()
    session.spark.sql(query.query6()).show()

    //setting up and running query 7
    var dfne  = session.spark.sql(query.queryNE())
    var dfne2 = dfne.withColumn("Region", lit("Northeast"))

    var dfsw  = session.spark.sql(query.querySW())
    var dfsw2 = dfsw.withColumn("Region", lit("Southwest"))

    var dfw1  = session.spark.sql(query.queryW())
    var dfw2 = dfw1.withColumn("Region", lit("West"))

    var dfse  = session.spark.sql(query.querySE())
    var dfse2 = dfse.withColumn("Region", lit("Southeast"))

    var dfmw  = session.spark.sql(query.queryMW())
    var dfmw2 = dfmw.withColumn("Region", lit("Midwest"))
    var total = dfne2.union(dfsw2).union(dfw2).union(dfse2).union(dfmw2)

    total.createOrReplaceTempView("region")

    session.spark.sql(query.query7()).show()

    //running query 8
    session.spark.sql(query.query8()).show()

    /*************************QUERY FOR POPULATION OF DIFFERENT CATEGORIES**************************************/
    //sets up columns for query 9
    var dse1 = session.spark.emptyDataFrame
    var dse2 = session.spark.emptyDataFrame
    var dse3 = session.spark.emptyDataFrame

    val datas = Seq(Row("Total 2000"))
    val schemas = new StructType()
      .add("Year",StringType)
    val df2000 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(datas),schemas)
    dse1 = df2000

    val data2s = Seq(Row("Total 2010"))
    val schema2s = new StructType()
      .add("Data2010",StringType)
    val df2010 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data2s),schema2s)
    dse2 = df2010

    val data3s = Seq(Row("Total 2020"))
    val schema3s = new StructType()
      .add("Data2020",StringType)
    val df2020 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data3s),schema3s)
    dse3 = df2020

    df3.createOrReplaceTempView("Testing1Imp")
    df2.createOrReplaceTempView("Testing2Imp")
    df.createOrReplaceTempView("Testing3Imp")

    val headers = session.spark.read.format("csv").option("header","true").load(s"s3a://$bucket/tableFiles/headers.csv")
    headers.createOrReplaceTempView("HeaderImp")

    var testingS = df3.drop("FILEID", "STUSAB", "Region", "Division", "CHARITER", "CIFSN", "LOGRECNO", "State")

    var ColumnNames = testingS.columns
    var Columnstring = ColumnNames.mkString("sum(", "),sum(", ")")
    var Columnstring2 = ColumnNames.mkString(",")
    var Columnlist = Columnstring.split(",")

    var HeaderNames = headers.columns
    var Headerstring = HeaderNames.mkString(",")
    var Headerlist = Headerstring.split(",")

    var newdata1 = session.spark.sql(s"SELECT $Columnstring FROM Testing1Imp").toDF()
    var newdata2 = session.spark.sql(s"SELECT $Columnstring FROM Testing2Imp").toDF()
    var newdata3 = session.spark.sql(s"SELECT $Columnstring FROM Testing3Imp").toDF()

    //2000
    dse1 = dse1.join(newdata1)
    //2010
    dse2 = dse2.join(newdata2)
    //2020
    dse3 = dse3.join(newdata3)

    var Joining = dse1.union(dse2)
    var Joining2 = Joining.union(dse3)

    var lastimp = Columnlist.length

    for ( i <- 0 until lastimp){

      var FinalTable = Joining2.withColumnRenamed(s"${Columnlist(i)}",f"${Headerlist(i).dropRight(1)}")
      Joining2 = FinalTable

    }
    //Join2.show()
    val df123 = Joining2
    //df123.show()


    /**********************************************************************************************************/
    //creating the empty dataframes for future use

    var dfe1 = session.spark.emptyDataFrame
    var dfe2 = session.spark.emptyDataFrame
    var dfe3 = session.spark.emptyDataFrame
    var dfe4 = session.spark.emptyDataFrame
    var dfe5 = session.spark.emptyDataFrame
    var dfe6 = session.spark.emptyDataFrame

    val half2 = df123.select(df123.columns.slice(73,151).map(m=>col(m)):_*)
    half2.repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/queries/half2/")

    //white
    val data = Seq(Row("White"))
    val schema = new StructType()
      .add("Years",StringType)
    val dfwhite = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data),schema)
    dfe1 = dfwhite

    //black
    val data2 = Seq(Row("Black"))
    val schema2 = new StructType()
      .add("Years",StringType)
    val dfblack = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data2),schema2)
    dfe2 = dfblack

    //Asian
    val data3 = Seq(Row("Asian"))
    val schema3 = new StructType()
      .add("Years",StringType)
    val dfasian = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data3),schema3)
    dfe3 = dfasian

    //Native
    val data4 = Seq(Row("Native"))
    val schema4 = new StructType()
      .add("Years",StringType)
    val dfnative = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data4),schema4)
    dfe4 = dfnative

    //hispanic
    val data5 = Seq(Row("Hispanic"))
    val schema5 = new StructType()
      .add("Year",StringType)
    val dfhispanic = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data5),schema5)
    dfe5 = dfhispanic

    //for making csv
    val data6 = Seq(Row("2000"), Row("2010"), Row("2020"))
    val schemap = new StructType()
      .add("years",StringType)
    dfe6 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data6), schemap)
    val windowSpec2 = Window.orderBy(asc("years"))
    dfe6 = dfe6.withColumn("id", row_number.over(windowSpec2))

    //creation of dataframe to pivot/transpose on while adding an id column
    var half2ori = session.spark.read.option("header", "true").csv(s"s3a://$bucket/tableFiles/queries/half2/")
    val windowSpec = Window.orderBy(asc("HispanicorLatin"))
    half2ori = half2ori.withColumn("id", row_number.over(windowSpec))


    var half2ori2 = dfe6.join(half2ori, dfe6("id") === half2ori("id"), "left").drop("id")

    //creation of pivoted dataframe without race column
    val schemapivot = new StructType()
      .add("y2000",StringType)
      .add("y2010",StringType)
      .add("y2020",StringType)
    var dfpivot = session.spark.createDataFrame(session.spark.sparkContext.emptyRDD[Row], schemapivot)
    var half2oricolumn = half2ori2.columns
    var bool = false
    for(i <- half2oricolumn){
      if(bool) {
        dfpivot = dfpivot.union(half2ori2.groupBy().pivot("years").agg(first(i)))
      }else{
        bool = true
      }
    }

    var dfpivot2 = dfpivot.withColumn("Id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

    //creation of race column with id column
//    val schemarace = new StructType()
//      .add("Race", StringType)
//    var dfrace = session.spark.createDataFrame(session.spark.sparkContext.emptyRDD[Row], schemarace)
//    val testlist = half2oricolumn.toList
//    val columns = Seq("HispanicorLatin",	"NotHispanicorLatin",	"Populationofonerace7",	"Whitealone7",	"BlackorAfricanAmericanalone7",	"AmericanIndianandAlaskaNativealone7",	"Asianalone7",	"NativeHawaiianandOtherPacificIslanderalone7",	"SomeOtherRacealone8",	"Populationoftwoormoreraces8",	"Populationoftworaces8",	"WhiteBlackorAfricanAmerican8",	"WhiteAmericanIndianandAlaskaNative8",	"WhiteAsian8",	"WhiteNativeHawaiianandOtherPacificIslander8",	"WhiteSomeOtherRace8",	"BlackorAfricanAmericanAmericanIndianandAlaskaNative8",	"BlackorAfricanAmericanAsian8",	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslander9",	"BlackorAfricanAmericanSomeOtherRace9",	"AmericanIndianandAlaskaNativeAsian9",	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander9",	"AmericanIndianandAlaskaNativeSomeOtherRace9",	"AsianNativeHawaiianandOtherPacificIslander9",	"AsianSomeOtherRace9",	"NativeHawaiianandOtherPacificIslanderSomeOtherRace9",	"Populationofthreeraces9",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNative9",	"WhiteBlackorAfricanAmericanAsian10",	"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslander10",	"WhiteBlackorAfricanAmericanSomeOtherRace10",	"WhiteAmericanIndianandAlaskaNativeAsian10",	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander10",	"WhiteAmericanIndianandAlaskaNativeSomeOtherRace10"	,"WhiteAsianNativeHawaiianandOtherPacificIslander10",	"WhiteAsianSomeOtherRace10",	"WhiteNativeHawaiianandOtherPacificIslanderSomeOtherRac",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian10",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander11",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace11",	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander11",	"BlackorAfricanAmericanAsianSomeOtherRace11",	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander11"	,"AmericanIndianandAlaskaNativeAsianSomeOtherRace11",	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"AsianNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"Populationoffourraces11",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian12",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander12",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace12",	"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander12",	"WhiteBlackorAfricanAmericanAsianSomeOtherRace12"	,"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander12",	"WhiteAmericanIndianandAlaskaNativeAsianSomeOtherRace12",	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"WhiteAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"Populationoffiveraces13",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13"	,"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	,"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	,"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14"	,"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14",	"Populationofsixraces14"	,"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14")
//    val rdd = session.spark.sparkContext.parallelize(columns)
//    val rowRDD = rdd.map(attributes => Row(attributes))
//    val dfFromRDD3 = session.spark.createDataFrame(rowRDD,schemarace)
//
//    val dataFrame2 = dfFromRDD3.withColumn("Id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
//
//    //creation of splittingthe9 table for query9
//    var splittingthe9 = dataFrame2.join(dfpivot2, dataFrame2("Id") === dfpivot2("Id"), "left").drop("Id")
//
//
//    splittingthe9.createOrReplaceTempView("sp9")

    val dfw = session.spark.sql("SELECT sum(y2000) AS pop2000, sum(y2010) AS pop2010, sum(y2020) AS pop2020 FROM sp9 WHERE Race LIKE '%White%'")
    val dfb = session.spark.sql("SELECT sum(y2000) AS pop2000, sum(y2010) AS pop2010, sum(y2020) AS pop2020 FROM sp9 WHERE Race LIKE '%Black%'")
    val dfa = session.spark.sql("SELECT sum(y2000) AS pop2000, sum(y2010) AS pop2010, sum(y2020) AS pop2020 FROM sp9 WHERE Race LIKE '%Asian%'")
    val dfn =session.spark.sql("SELECT sum(y2000) AS pop2000, sum(y2010) AS pop2010, sum(y2020) AS pop2020 FROM sp9 WHERE Race LIKE '%Native%'")
    val dfh = session.spark.sql("SELECT sum(y2000) AS pop2000, sum(y2010) AS pop2010, sum(y2020) AS pop2020 FROM sp9 WHERE Race LIKE 'Hispanic%'")

    val dfc1 = dfe1.join(dfw)
    val dfc2 = dfe2.join(dfb)
    val dfc3 = dfe3.join(dfa)
    val dfc4 = dfe4.join(dfn)
    val dfc5 = dfe5.join(dfh)

    val Join2 = dfc1.union(dfc2).union(dfc3).union(dfc4).union(dfc5)
    Join2.show()

    /**********************************************************************************************************************************/

    session.spark.sql(query.query1()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query1/")
    session.spark.sql(query.query2()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query2/")
    session.spark.sql(query.query3()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query3/")
    q3f.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query3ordered/")
    session.spark.sql(query.query4()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query4/")
    session.spark.sql(query.query5()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query5/")
    session.spark.sql(query.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query6/")
    session.spark.sql(query.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query6/")
    session.spark.sql(query.query7()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query7/")
    session.spark.sql(query.query8()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query8/")
    Join2.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query9/")

    //Code for future analysis
    val fullDf =session.spark.sql(" SELECT DISTINCT(f.STUSAB) , f.P0010001  , s.P0010001  , t.P0010001" +
      " FROM c2000  f INNER JOIN c2010 s ON f.STUSAB =s.STUSAB INNER JOIN c2020 t ON s.STUSAB=t.STUSAB Order By f.STUSAB  ").toDF("STUSAB", "Pop2000", "Pop2010", "Pop2020")

    //deletes files if already exists
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/decayProjectionStates/decayProjectionStates.csv")
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/hybridProjectionStates/hybridProjectionStates.csv")
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/slopeProjection/slopeProjection.csv")
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/decayProjectionUS/decayProjectionUS.csv")
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/decayHybridProjectionUS/decayHybridProjectionUS.csv")
    deleteFile(s"s3a://$bucket/tableFiles/resultCsv/slopeProjectionUS/slopeProjectionUS.csv")

    //adds the needed headers to projection files
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/decayProjectionStates/decayProjectionStates.csv")
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/hybridProjectionStates/hybridProjectionStates.csv")
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/slopeProjection/slopeProjection.csv")
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/decayProjectionUS/decayProjectionUS.csv")
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/decayHybridProjectionUS/decayHybridProjectionUS.csv")
    AddHeader(s"s3a://$bucket/tableFiles/resultCsv/slopeProjectionUS/slopeProjectionUS.csv")

    //gets populations as lists
    val list2010=fullDf.select("Pop2010").collectAsList()
    val list2000=fullDf.select("Pop2000").collectAsList()
    val list2020=fullDf.select("Pop2020").collectAsList()
    val statecode=fullDf.select("STUSAB").collectAsList()

    //creates population projections for each state
    for(i <- 0 to list2020.length-1) {

      val pop2000Formatted = list2000(i)(0).toString.substring(0, list2000(i)(0).toString.length()-2).toLong
      val pop2010Formatted = list2010(i)(0).toString.substring(0, list2010(i)(0).toString.length()-2).toLong
      val pop2020Formatted = list2020(i)(0).toString.substring(0, list2020(i)(0).toString.length()-2).toLong

      //passing parameter to prediction function using loop, This example passing value of Column name "P0010001"
      decayProjection(statecode(i)(0).toString, pop2000Formatted, pop2010Formatted, pop2020Formatted, s"s3a://$bucket/tableFiles/resultCsv/decayProjectionStates/decayProjectionStates.csv")
      decayHybridProjection(statecode(i)(0).toString, pop2000Formatted, pop2010Formatted, pop2020Formatted, s"s3a://$bucket/tableFiles/resultCsv/hybridProjectionStates/hybridProjectionStates.csv")
      slopeProjection(statecode(i)(0).toString, pop2000Formatted, pop2010Formatted, pop2020Formatted, s"s3a://$bucket/tableFiles/resultCsv/slopeProjection/slopeProjection.csv")
    }

    //creates population projection for the US
    val yearsDf = session.spark.read.option("header", "true").csv(s"s3a://$bucket/tableFiles/resultCsv/query1")
    val totalPopList = yearsDf.collectAsList()

    val USpop2000Formatted = totalPopList(0)(0).toString().substring(0, totalPopList(0)(0).toString.length()-2).toLong
    val USpop2010Formatted = totalPopList(0)(1).toString().substring(0, totalPopList(0)(1).toString.length()-2).toLong
    val USpop2020Formatted = totalPopList(0)(2).toString().substring(0, totalPopList(0)(2).toString.length()-2).toLong

    decayProjection("US", USpop2000Formatted, USpop2010Formatted, USpop2020Formatted, s"s3a://$bucket/tableFiles/resultCsv/decayProjectionUS/decayProjectionUS.csv")
    decayHybridProjection("US", USpop2000Formatted, USpop2010Formatted, USpop2020Formatted, s"s3a://$bucket/tableFiles/resultCsv/decayHybridProjectionUS/decayHybridProjectionUS.csv")
    slopeProjection("US", USpop2000Formatted, USpop2010Formatted, USpop2020Formatted, s"s3a://$bucket/resultCsv/slopeProjectionUS/slopeProjectionUS.csv")

  }
}
