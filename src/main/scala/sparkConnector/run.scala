package sparkConnector

import com.amazonaws.services.s3.model.GetObjectRequest
import etl.{andyCombinedScrapper, transforming}

import java.io.File

object run extends App {
  val bucket = "revature-william-big-data-1377"
  val session = new sparkConnector.spark()


  andyCombinedScrapper.run()
  transforming.run()

  session.client.putObject(bucket, "csvraw/Combine2000RG.csv", new File("./OutputCSV2/2000.csv"))
  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2000RG.csv"),
    (new File("csvraw/Combine2000RG.csv")))

  session.client.putObject(bucket, "csvraw/Combine2010RG.csv", new File("./OutputCSV2/2010.csv"))
  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2010RG.csv"),
    (new File("csvraw/Combine2010RG.csv")))

  session.client.putObject(bucket, "csvraw/Combine2020RG.csv", new File("./OutputCSV2/2020.csv"))
  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2020RG.csv"),
    (new File("csvraw/Combine2020RG.csv")))

  println(session.client.getUrl(bucket, "csvraw/Combine2000RG.csv"))
  println(session.client.getUrl(bucket, "csvraw/Combine2010RG.csv"))
  println(session.client.getUrl(bucket, "csvraw/Combine2020RG.csv"))

}
