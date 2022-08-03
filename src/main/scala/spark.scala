import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.{GREEN, RESET}
import scala.io.{BufferedSource, Source}

class spark (){

  val bucket = "revature-william-big-data-1377"

  var accessKey = " "
  var secretKey = " "
  val bufferedSource: BufferedSource = Source.fromFile("C:\\Resources\\rootkeyP3.csv")
  var count = 0
  for (line <- bufferedSource.getLines) {
    val Array(val1, value) = line.split("=").map(_.trim)
    count match {
      case 0 => accessKey = value
      case 1 => secretKey = value
    }
    count = count + 1
  }

  //System.setProperty("hadoop.home.dir", "C:\\hadoop3")
  val spark = SparkSession
    .builder
    .appName("Spark Queries")
    .master("local[*]")
    //.config("spark.master", "local[*]")   // possibly use for remote master connection
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .config("spark.hadoop.fs.s3a.access.key", accessKey)
    .config("spark.hadoop.fs.s3a.secret.key", secretKey)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  //PropertyConfigurator.configure("log4j.properties")
  val logger: Logger = org.apache.log4j.Logger.getRootLogger
  //println(" spark session")
  logger.info(s"$GREEN Created Spark Session$RESET")

  val creds:BasicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
  val client:AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(Regions.US_EAST_1).build()


  accessKey = " "
  secretKey = " "

}