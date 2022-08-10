package sparkConnector

import etl.{andyFixedETL, andyOptimizedScrapper}

object run extends App {
  val bucket = "revature-william-big-data-1377"
  val session = new sparkConnector.spark()


  andyOptimizedScrapper.run()

  andyFixedETL.ETLfunction()
//
//  session.client.putObject(bucket, "csvraw/Combine2000RG.csv", new File("./OutputCSV2/2000.csv"))
//  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2000RG.csv"),
//    (new File("csvraw/Combine2000RG.csv")))
//
//  session.client.putObject(bucket, "csvraw/Combine2010RG.csv", new File("./OutputCSV2/2010.csv"))
//  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2010RG.csv"),
//    (new File("csvraw/Combine2010RG.csv")))
//
//  session.client.putObject(bucket, "csvraw/Combine2020RG.csv", new File("./OutputCSV2/2020.csv"))
//  session.client.getObject(new GetObjectRequest(bucket, "csvraw/Combine2020RG.csv"),
//    (new File("csvraw/Combine2020RG.csv")))

  println(session.client.getUrl(bucket, "csvraw/Combine2000RG.csv"))
  println(session.client.getUrl(bucket, "csvraw/Combine2010RG.csv"))
  println(session.client.getUrl(bucket, "csvraw/Combine2020RG.csv"))

}
