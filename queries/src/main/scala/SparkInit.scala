import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

class SparkInit (appName:String){

  val spark = SparkSession
    .builder
    .appName(appName)
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("aka").setLevel(Level.OFF)

  val logger: Logger = org.apache.log4j.Logger.getRootLogger()
}
