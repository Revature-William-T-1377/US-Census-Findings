package sparkConnector

import etl.{ETLOperations, optimizedScraper}

object run extends App{
  val bucket = "revature-william-big-data-1377"
  val session = new sparkConnector.spark()

  optimizedScraper.run()
  ETLOperations.ETLfunction()
  optimizedScraper.deleteExtraFiles()

}
