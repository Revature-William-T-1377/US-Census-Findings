package sparkConnector

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class sparkCxn {

  val spkSession: SparkSession = SparkSession
    .builder
    .appName("Spark Remote")
    .master("spark://69.145.20.199:7077")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2048M")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.eventLog.enabled", value = true)
    .enableHiveSupport()
    .getOrCreate()

  // IDE log settings
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getRootLogger.setLevel(Level.WARN)

  println("~~ Created Spark Session ~~")

}
