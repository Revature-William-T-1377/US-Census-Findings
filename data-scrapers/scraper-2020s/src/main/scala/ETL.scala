import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.{File, FileInputStream, FileOutputStream, FileWriter, InputStream, PrintWriter}
import java.util.zip.ZipInputStream
import scala.io.Source
import util.Try
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import Console.{CYAN, YELLOW, RED, WHITE, RESET, GREEN}
import scala.util.Random
import org.apache.spark.sql
import scala.io.StdIn.readLine

object ETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("big gay")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("aka").setLevel(Level.OFF)

    val df = spark.read.option("header", "true").csv("datasets/usgeo2020.csv")
    df.createOrReplaceTempView("ETL")
    var df1 = spark.read.option("header", "true").csv("datasets/us00001.csv")
    df1 = df1.drop("FILEID", "CHARITER", "CIFSN")
    df1.createOrReplaceTempView("raceData")

    val geodata = spark.sql("SELECT STUSAB as TAG, LOGRECNO as RECORD, SUMLEV, BASENAME as NAME, POP100 as POPULATION FROM ETL WHERE LOGRECNO < 67")
    geodata.createOrReplaceTempView("geodata")
    var data = spark.sql("SELECT * FROM geodata JOIN raceData ON geodata.RECORD = raceData.LOGRECNO")
    data = data.drop("STUSAB", "LOGRECNO")
    data.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("datasets/cleaned")
  }
}
