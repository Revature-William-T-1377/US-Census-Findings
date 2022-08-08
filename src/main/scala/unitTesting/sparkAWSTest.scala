package unitTesting

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.{GREEN, RESET}
import scala.io.{BufferedSource, Source}

class sparkAWSTest {
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
  val spark: SparkSession = SparkSession.builder()
    .appName("Spark AWS Connect")
    //.config("spark.master", "local[*]")

    .master("spark://69.145.20.199:7077")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2048M")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.eventLog.enabled", value = true)

    .config("spark.hadoop.fs.s3a.access.key", accessKey)
    .config("spark.hadoop.fs.s3a.secret.key", secretKey)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .enableHiveSupport()
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("aka").setLevel(Level.OFF)
  //PropertyConfigurator.configure("log4j.properties")
  val logger: Logger = org.apache.log4j.Logger.getRootLogger
  //println(" spark session")
  logger.info(s"$GREEN Created Spark Session$RESET")

  val creds: BasicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  val client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(Regions.US_EAST_1).build()

}
