package sparkConnector

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.{BufferedSource, Source}

object sparkQueries extends App {
  val spark: SparkSession = sparkCxn()

  def sparkCxn(): SparkSession = {
    var accessKey = " "
    var secretKey = " "

    val bufferedSource: BufferedSource = Source.fromFile("/home/gentooadmin/proj3/rootkey.csv")
    var count = 0
    for (line <- bufferedSource.getLines) {
      val Array(val1, value) = line.split("=").map(_.trim)
      count match {
        case 0 => accessKey = value
        case 1 => secretKey = value
      }
      count = count + 1
    }

    val spark = SparkSession
      .builder
      .appName("Spark queries.Queries")
      .master("local[*]")
      //.config("spark.master", "local[*]")   // possibly use for remote master connection
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val logger: Logger = org.apache.log4j.Logger.getRootLogger

    spark
  }

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def testQuery(): Unit = {
    // define schema structure; column names based on json data
    val schema = StructType(
      Array(
        StructField("KEY", StringType, nullable = false),
        StructField("COLUMN1", StringType, nullable = false),
        StructField("COLUMN2", StringType, nullable = false),
        StructField("COLUMN3", StringType, nullable = false)
      )

    )

    // path to test data in project test bucket (AWS S3)
    val urlfile = "https://revature-william-big-data-1377.s3.amazonaws.com/testfolder/test.json"
    spark.sparkContext.addFile(urlfile)

    // create dataframe to read json
    val dataframe = spark
      .read
      .schema(schema)
      .format("json") // may specify csv here
      .option("header", "true")
      .load("file://" + SparkFiles.get("test.json")) // match filename with urlfile

    dataframe.show()
  }

  //testQuery()

  def exampleQuery(): Unit = {
    // define schema structure; column names based on data columns
    val schema = StructType(
      Array(
        StructField("KEY", StringType, nullable = false),
        StructField("COLUMN1", StringType, nullable = false),
        StructField("COLUMN2", StringType, nullable = false),
        StructField("COLUMN3", StringType, nullable = false)
      )

    )

    val csvname = "Combine2000RG.csv"
    //val csvname = "Combine2010RG.csv"
    //val csvname = "Combine2020RG.csv"

    // path to test data in project bucket (AWS S3)
    val urlfile = s"https://revature-william-big-data-1377.s3.amazonaws.com/csvraw/$csvname"
    spark.sparkContext.addFile(urlfile)

    // create dataframe to read json
    val dataframe = spark
      .read
      //.schema(schema)
      .format("csv")
      .option("header", "true")
      .load("file://" + SparkFiles.get(s"$csvname")) // match filename with urlfile

    dataframe.show()


    //    //second method for getting from bucket
    //    val bucket = "revature-william-big-data-1377"
    //    val df = spark.read.format("csv").option("header", "true").load(s"s3a://$bucket/csvraw/Combine2020RG.csv") // File location in hdfs
    //    df.show()

  }

  exampleQuery()

}
