import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
object FutureTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FutureTest")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
  }
}
