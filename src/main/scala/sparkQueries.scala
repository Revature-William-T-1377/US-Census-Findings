
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.language.postfixOps

object sparkQueries extends App {
  val session = new spark()

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
    session.spark.sparkContext.addFile(urlfile)

    // create dataframe to read json
    val dataframe = session.spark
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
    session.spark.sparkContext.addFile(urlfile)

    // create dataframe to read json
    val dataframe = session.spark
      .read
      //.schema(schema)
      .format("csv")
      .option("header", "true")
      .load("file://" + SparkFiles.get(s"$csvname")) // match filename with urlfile

    dataframe.show()


      //second method for getting from bucket
    val bucket = "revature-william-big-data-1377"
    val df = session.spark.read.format("csv").option("header", "true").load(s"s3a://$bucket/csvraw/Combine2020RG.csv") // File location in hdfs
    df.show()

  }

  exampleQuery()

}
