import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.{asc, col, first, lit, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import sparkConnector.spark

object splittingthe9 extends App {
  val session = new spark()
  var dfe1 = session.spark.emptyDataFrame
  var dfe2 = session.spark.emptyDataFrame
  var dfe3 = session.spark.emptyDataFrame
  var dfe4 = session.spark.emptyDataFrame
  var dfe5 = session.spark.emptyDataFrame
  var dfe6 = session.spark.emptyDataFrame
  var dfe7 = session.spark.emptyDataFrame
  var dfe8 = session.spark.emptyDataFrame



  //creating of half1/half2
  /*var df = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\US-Census-Findings\\src\\main\\scala\\queries\\query9.csv")

  val half1 = df.select(df.columns.slice(3,73).map(m=>col(m)):_*)
  val half2 = df.select(df.columns.slice(73,151).map(m=>col(m)):_*)

  half1.repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("queries/half1/")
  half2.repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("queries/half2/")*/

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
  /*var half2 = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\US-Census-Findings\\queries\\half2\\half2.csv")
  half2.createOrReplaceTempView("half2")*/

  var half2ori = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\US-Census-Findings\\queries\\half2\\part-00000-56d40e52-9d66-4b46-aa41-ccdfea3cbe9b-c000.csv")
  val windowSpec = Window.orderBy(asc("HispanicorLatin"))
  half2ori = half2ori.withColumn("id", row_number.over(windowSpec))


  var half2ori2 = dfe6.join(half2ori, dfe6("id") === half2ori("id"), "left").drop("id")
  half2ori2.show()
  /*var r2000 = session.spark.sql("Select * from half2ori where WhiteAsianSomeOtherRace10 = 21990.0").collectAsList().get(0)
  for(i <- 0 to r2000.length-1){
    println(r2000.get(i))
  }*/
  //half2ori.printSchema()
  /*var half2oricolumn = half2ori2.columns
  for(i <- half2oricolumn){
    half2ori2 = half2ori2.withColumn(i,col(i).cast(IntegerType))
  }*/
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
  /*val windowSpec3 = Window.orderBy("y2000")
  dfpivot = dfpivot.withColumn("id", row_number.over(windowSpec3))
  dfpivot.show(1000)
  System.exit(0)*/

  /*val dataFrame1 = dfpivot.withColumn("index",monotonically_increasing_id())
  dataFrame1.show(1000)*/
  import session.spark.sqlContext.implicits._

  var dfpivot2 = dfpivot.withColumn("Id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
  dfpivot2.show()



  val schemarace = new StructType()
    .add("Race", StringType)
  var dfrace = session.spark.createDataFrame(session.spark.sparkContext.emptyRDD[Row], schemarace)
  val testlist = half2oricolumn.toList
  val columns = Seq("HispanicorLatin",	"NotHispanicorLatin",	"Populationofonerace7",	"Whitealone7",	"BlackorAfricanAmericanalone7",	"AmericanIndianandAlaskaNativealone7",	"Asianalone7",	"NativeHawaiianandOtherPacificIslanderalone7",	"SomeOtherRacealone8",	"Populationoftwoormoreraces8",	"Populationoftworaces8",	"WhiteBlackorAfricanAmerican8",	"WhiteAmericanIndianandAlaskaNative8",	"WhiteAsian8",	"WhiteNativeHawaiianandOtherPacificIslander8",	"WhiteSomeOtherRace8",	"BlackorAfricanAmericanAmericanIndianandAlaskaNative8",	"BlackorAfricanAmericanAsian8",	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslander9",	"BlackorAfricanAmericanSomeOtherRace9",	"AmericanIndianandAlaskaNativeAsian9",	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander9",	"AmericanIndianandAlaskaNativeSomeOtherRace9",	"AsianNativeHawaiianandOtherPacificIslander9",	"AsianSomeOtherRace9",	"NativeHawaiianandOtherPacificIslanderSomeOtherRace9",	"Populationofthreeraces9",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNative9",	"WhiteBlackorAfricanAmericanAsian10",	"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslander10",	"WhiteBlackorAfricanAmericanSomeOtherRace10",	"WhiteAmericanIndianandAlaskaNativeAsian10",	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander10",	"WhiteAmericanIndianandAlaskaNativeSomeOtherRace10"	,"WhiteAsianNativeHawaiianandOtherPacificIslander10",	"WhiteAsianSomeOtherRace10",	"WhiteNativeHawaiianandOtherPacificIslanderSomeOtherRac",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian10",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander11",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace11",	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander11",	"BlackorAfricanAmericanAsianSomeOtherRace11",	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander11"	,"AmericanIndianandAlaskaNativeAsianSomeOtherRace11",	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"AsianNativeHawaiianandOtherPacificIslanderSomeOtherRace11",	"Populationoffourraces11",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian12",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander12",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace12",	"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander12",	"WhiteBlackorAfricanAmericanAsianSomeOtherRace12"	,"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander12",	"WhiteAmericanIndianandAlaskaNativeAsianSomeOtherRace12",	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"WhiteAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace12",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13",	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13",	"Populationoffiveraces13",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13"	,"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13",	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	,"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	,"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14"	,"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14",	"Populationofsixraces14"	,"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14")
  val rdd = session.spark.sparkContext.parallelize(columns)
  val rowRDD = rdd.map(attributes => Row(attributes))
  val dfFromRDD3 = session.spark.createDataFrame(rowRDD,schemarace)
  dfFromRDD3.show(1000)
  //"HispanicorLatin"	"NotHispanicorLatin"	"Populationofonerace7"	"Whitealone7"	"BlackorAfricanAmericanalone7"	"AmericanIndianandAlaskaNativealone7"	"Asianalone7"	"NativeHawaiianandOtherPacificIslanderalone7"	"SomeOtherRacealone8"	"Populationoftwoormoreraces8"	"Populationoftworaces8"	"WhiteBlackorAfricanAmerican8"	"WhiteAmericanIndianandAlaskaNative8"	"WhiteAsian8"	"WhiteNativeHawaiianandOtherPacificIslander8"	"WhiteSomeOtherRace8"	"BlackorAfricanAmericanAmericanIndianandAlaskaNative8"	"BlackorAfricanAmericanAsian8"	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslander9"	"BlackorAfricanAmericanSomeOtherRace9"	"AmericanIndianandAlaskaNativeAsian9"	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander9"	"AmericanIndianandAlaskaNativeSomeOtherRace9"	"AsianNativeHawaiianandOtherPacificIslander9"	"AsianSomeOtherRace9"	"NativeHawaiianandOtherPacificIslanderSomeOtherRace9"	"Populationofthreeraces9"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNative9"	"WhiteBlackorAfricanAmericanAsian10"	"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslander10"	"WhiteBlackorAfricanAmericanSomeOtherRace10"	"WhiteAmericanIndianandAlaskaNativeAsian10"	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander10"	"WhiteAmericanIndianandAlaskaNativeSomeOtherRace10"	"WhiteAsianNativeHawaiianandOtherPacificIslander10"	"WhiteAsianSomeOtherRace10"	"WhiteNativeHawaiianandOtherPacificIslanderSomeOtherRac"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian10"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander11"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace11"	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander11"	"BlackorAfricanAmericanAsianSomeOtherRace11"	"BlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace11"	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander11"	"AmericanIndianandAlaskaNativeAsianSomeOtherRace11"	"AmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace11"	"AsianNativeHawaiianandOtherPacificIslanderSomeOtherRace11"	"Populationoffourraces11"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsian12"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslander12"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeSomeOtherRace12"	"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslander12"	"WhiteBlackorAfricanAmericanAsianSomeOtherRace12"	"WhiteBlackorAfricanAmericanNativeHawaiianandOtherPacificIslanderSomeOtherRace12"	"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander12"	"WhiteAmericanIndianandAlaskaNativeAsianSomeOtherRace12"	"WhiteAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace12"	"WhiteAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace12"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	"BlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	"AmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	"Populationoffiveraces13"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslander13"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianSomeOtherRace13"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	"WhiteBlackorAfricanAmericanAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace13"	"WhiteAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14"	"BlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14"	"Populationofsixraces14"	"WhiteBlackorAfricanAmericanAmericanIndianandAlaskaNativeAsianNativeHawaiianandOtherPacificIslanderSomeOtherRace14"
  val dataFrame2 = dfFromRDD3.withColumn("Id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
  dataFrame2.show(1000)

  var splittingthe9 = dataFrame2.join(dfpivot2, dataFrame2("Id") === dfpivot2("Id"), "left").drop("Id")
  splittingthe9.show()

  splittingthe9.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./queries/splittingthe9/")
  splittingthe9.createOrReplaceTempView("sp9")

  /*val pivotdf1 = half2ori2.groupBy().pivot("years").agg(first("HispanicorLatin"))
  val pivotdf2 = half2ori2.groupBy().pivot("years").agg(first("NotHispanicorLatin"))
  pivotdf1.show()
  pivotdf2.show()
  pivotdf1.union(pivotdf2).show()*/

val dfw = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM sp9 WHERE Race LIKE '%White%'")
val dfb = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM sp9 WHERE Race LIKE '%Black%'")
val dfa = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM sp9 WHERE Race LIKE '%Asian%'")
val dfn =session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM sp9 WHERE Race LIKE '%Native%'")
val dfh = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM sp9 WHERE Race LIKE 'Hispanic%'")

val dfc1 = dfe1.join(dfw)
val dfc2 = dfe2.join(dfb)
val dfc3 = dfe3.join(dfa)
val dfc4 = dfe4.join(dfn)
val dfc5 = dfe5.join(dfh)

dfc1.union(dfc2).union(dfc3).union(dfc4).union(dfc5).show()

}//end of app
