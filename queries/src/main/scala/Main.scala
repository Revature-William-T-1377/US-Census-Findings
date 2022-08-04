import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import Console.{CYAN, GREEN, RED, RESET, WHITE, YELLOW}
import org.apache.spark.sql
import os.truncate

import scala.io.StdIn.readLine

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project3")
    var df = session.spark.read.option("header", "true").csv("Combine2020RG.csv")
    var df2 = session.spark.read.option("header", "true").csv("Combine2010RG.csv")
    var df3 = session.spark.read.option("header", "true").csv("Combine2000RG.csv")
    df = df.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df2 = df2.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))
    df3 = df3.withColumn("p0010001", col("p0010001").cast(DecimalType(18, 1)))

    //df3 = df3.withColumn("whitealone", col("whitealone").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("c2020")
    df2.createOrReplaceTempView("c2010")
    df3.createOrReplaceTempView("c2000")
    //df.withColumn("total", col("total").cast(DecimalType(18, 1)))
    //session.spark.sql("SELECT * [except total] FROM c2020").show()

    /*session.spark.sql("SELECT * FROM c2020").show()
    session.spark.sql("SELECT * FROM c2010").show()
    session.spark.sql("SELECT * FROM c2000").show()*/

    val queries = new Queries
    session.spark.sql(queries.query1()).show()
    session.spark.sql(queries.query2()).show()
    session.spark.sql(queries.query3()).show()
    session.spark.sql(queries.query4()).show()
    session.spark.sql(queries.query5()).show()
    session.spark.sql(queries.query6()).show()
    session.spark.sql(queries.query7()).show()


    session.spark.sql(queries.query1()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query1/")
    session.spark.sql(queries.query2()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query2/")
    session.spark.sql(queries.query3()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query3/")
    session.spark.sql(queries.query4()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query4/")
    session.spark.sql(queries.query5()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query5/")
    session.spark.sql(queries.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query6/")
    session.spark.sql(queries.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query6/")
    session.spark.sql(queries.query7()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query7/")
    session.spark.sql(queries.query8()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query8/")
    session.spark.sql(queries.query9()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query9/")
    session.spark.sql(queries.query10()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query10/")
  }
}
