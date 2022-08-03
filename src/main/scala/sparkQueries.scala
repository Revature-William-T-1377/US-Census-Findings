import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object sparkQueries extends App {
  val spark: SparkSession = sparkCxn()

  def sparkCxn(): SparkSession = {

    val spark = SparkSession
      .builder
      .appName("Spark Queries")
      .master("local[*]")
      //.config("spark.master", "local[*]")   // possibly use for remote master connection
      .config("spark.driver.allowMultipleContexts", "true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    println("~~ Created Spark Session ~~")

    // Return SparkSession
    spark
  }

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

  // dataframe to read json
  var dataframe = spark
    .read
    .schema(schema)
    .format("json")   // may specify csv here
    .option("header", "true")
    .load("file://" + SparkFiles.get("test.json"))    // match filename with urlfile

  dataframe.show()

}
