package sparkConnector

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.io.{BufferedSource, Source}

class sparkAuth {
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

  val creds: BasicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
  val client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withRegion(Regions.US_EAST_1).build()


}
