import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable._
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
    def projection(year1: Long, year2: Long, year3: Long): Array[Array[Long]]= {
      var years = Array(
      Array(2000, year1),
      Array(2010, year2),
      Array(2020, year3)
      )

      for(i <- 1 to 10) {
        var year1 :Double  = years(years.size - 3)(1)
        var year2 :Double = years(years.size - 2)(1)
        var year3 :Double = years(years.size - 1)(1)
        var growth1 = ((year2 - year1 )/ year2)
        println(growth1)
        var growth2 = ((year3 - year2) / year3)
        println(growth2)
        var derivative = (growth1 - growth2) // ???
        println(derivative)
        var year = 2020 + (i * 10)
        println(year)
        var population = years.last(1) + (years.last(1) * growth2 * derivative).toLong
        println(population)
        years = years :+ Array(year, population)
      }
      years
  }

  println(projection(4447100, 4779736, 5024279))
  }
}
