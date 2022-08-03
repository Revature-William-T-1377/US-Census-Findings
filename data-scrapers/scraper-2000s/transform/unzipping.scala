import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.io.Source

object unzipping {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")


    /*


    */



    //new File("D:\\Downloads\\Work\\data\\al000012010.pl").renameTo(new File("D:\\Downloads\\Work\\data\\al000012010.csv"))

    //new File("D:\\Downloads\\Work\\data\\al00001.upl").renameTo(new File("D:\\Downloads\\Work\\data\\al00001.csv"))

    //new File("D:\\Downloads\\Work\\data\\ca000012020.pl").renameTo(new File("D:\\Downloads\\Work\\data\\ca000012020.csv"))

    val df = spark.read.option("delimiter", "|").csv("D:\\Downloads\\Work\\data\\algeo201d0.pl")

    df.show()

    df.coalesce(1).write.csv("D:\\Downloads\\Work\\data\\testing\\algeo2020")

    //println(scala.io.Source.fromURL("https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/California/").mkString)

    // val states = https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/California/ca2020.pl.zip





    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration/1000000000) + " Seconds")
  }
}
