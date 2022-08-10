import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import sparkConnector.spark

object splittingthe9 extends App {
  val session = new spark()
  var dfe1 = session.spark.emptyDataFrame
  var dfe2 = session.spark.emptyDataFrame
  var dfe3 = session.spark.emptyDataFrame
  var dfe4 = session.spark.emptyDataFrame
  var dfe5 = session.spark.emptyDataFrame
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

  var half1 = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\US-Census-Findings\\queries\\half1\\half1.csv")
  var half2 = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\US-Census-Findings\\queries\\half2\\half2.csv")
  half1.createOrReplaceTempView("half1")
  half2.createOrReplaceTempView("half2")
  val dfw = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM half2 WHERE Race LIKE '%White%'")
  val dfb = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM half2 WHERE Race LIKE '%Black%'")
  val dfa = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM half2 WHERE Race LIKE '%Asian%'")
  val dfn =session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM half2 WHERE Race LIKE '%Native%'")
  val dfh = session.spark.sql("SELECT sum(y2000), sum(y2010), sum(y2020) FROM half2 WHERE Race LIKE 'Hispanic%'")

  val dfc1 = dfe1.join(dfw)
  val dfc2 = dfe2.join(dfb)
  val dfc3 = dfe3.join(dfa)
  val dfc4 = dfe4.join(dfn)
  val dfc5 = dfe5.join(dfh)

  dfc1.union(dfc2).union(dfc3).union(dfc4).union(dfc5).show()

}//end of app
