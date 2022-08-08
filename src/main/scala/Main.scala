import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import sparkConnector.spark

object Main {
  def main(args: Array[String]): Unit = {
    val session = new spark()
    val bucket = "revature-william-big-data-1377"
    var df = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2020RG.csv")//2020
    var df2 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2010RG.csv")//2010
    var df3 = session.spark.read.option("header", "true").csv(s"s3a://$bucket/csvraw/Combine2000RG.csv")//2000
    df = df.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))

    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")

    val query = queries.Queries
    session.spark.sql(query.query1()).show()
    session.spark.sql(query.query2()).show()
    session.spark.sql(query.query3()).show()
    session.spark.sql(query.query4()).show()
    session.spark.sql(query.query5()).show()
    session.spark.sql(query.query6()).show()
    var dfne  = session.spark.sql(query.queryNE())
    var dfne2 = dfne.withColumn("Region", lit("Northeast"))

    var dfsw  = session.spark.sql(query.querySW())
    var dfsw2 = dfsw.withColumn("Region", lit("Southwest"))

    var dfw  = session.spark.sql(query.queryW())
    var dfw2 = dfw.withColumn("Region", lit("West"))

    var dfse  = session.spark.sql(query.querySE())
    var dfse2 = dfse.withColumn("Region", lit("Southeast"))

    var dfmw  = session.spark.sql(query.queryMW())
    var dfmw2 = dfmw.withColumn("Region", lit("Midwest"))
    var total = dfne2.union(dfsw2).union(dfw2).union(dfse2).union(dfmw2)

    total.createOrReplaceTempView("region")

    session.spark.sql(query.query7()).show()
    session.spark.sql(query.query8()).show()

    /*************************QUERY FOR POPULATION OF DIFFERENT CATEGORIES**************************************/
    var dfe1 = session.spark.emptyDataFrame
    var dfe2 = session.spark.emptyDataFrame
    var dfe3 = session.spark.emptyDataFrame

    val data = Seq(Row("Total 2000"))
    val schema = new StructType()
      .add("Year",StringType)
    val df2000 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data),schema)
    dfe1 = df2000

    val data2 = Seq(Row("Total 2010"))
    val schema2 = new StructType()
      .add("Data2010",StringType)
    val df2010 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data2),schema2)
    dfe2 = df2010

    val data3 = Seq(Row("Total 2020"))
    val schema3 = new StructType()
      .add("Data2020",StringType)
    val df2020 = session.spark.createDataFrame(session.spark.sparkContext.parallelize(data3),schema3)
    dfe3 = df2020

    //val testing1 = session.spark.read.format("csv").option("header","true").load("src/main/scala/queries/Combine2000RG.csv") // File location in hdfs
    df3.createOrReplaceTempView("Testing1Imp")

    //val testing2 = session.spark.read.format("csv").option("header","true").load("src/main/scala/queries/Combine2010RG.csv") // File location in hdfs
    df2.createOrReplaceTempView("Testing2Imp")

    //val testing3 = session.spark.read.format("csv").option("header","true").load("src/main/scala/queries/Combine2020RG.csv") // File location in hdfs
    df.createOrReplaceTempView("Testing3Imp")

    val headers = session.spark.read.format("csv").option("header","true").load(s"s3a://$bucket/csvraw/headers.csv") // File location in hdfs
    headers.createOrReplaceTempView("HeaderImp")

    var testingS = df3.drop("FILEID", "STUSAB", "Region", "Division", "CHARITER", "CIFSN", "LOGRECNO")

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
    dfe1 = dfe1.join(newdata1)
    //2010
    dfe2 = dfe2.join(newdata2)
    //2020
    dfe3 = dfe3.join(newdata3)

    var Join1 = dfe1.union(dfe2)

    var Join2 = Join1.union(dfe3)


    var lastimp = Columnlist.length

    for ( i <- 0 until lastimp){

      var FinalTable = Join2.withColumnRenamed(s"${Columnlist(i)}",f"${Headerlist(i).dropRight(1)}")
      Join2 = FinalTable

    }
    Join2.show()


    /**********************************************************************************************************************************/


    session.spark.sql(query.query1()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query1/")
    session.spark.sql(query.query2()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query2/")
    session.spark.sql(query.query3()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query3/")
    session.spark.sql(query.query4()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query4/")
    session.spark.sql(query.query5()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query5/")
    session.spark.sql(query.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query6/")
    session.spark.sql(query.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query6/")
    session.spark.sql(query.query7()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query7/")
    session.spark.sql(query.query8()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query8/")
    Join2.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query9/")

  }
}
