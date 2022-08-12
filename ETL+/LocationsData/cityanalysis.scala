import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scala.language.postfixOps
import org.apache.spark.sql.DataFrame

object cityanalysis {
  var t1 = System.nanoTime
  var startgeo: DataFrame = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")

    val locations = Array(
      Array("Alabama", "al"),
      Array("Alaska", "ak"),
      Array("Arizona", "az"),
      Array("Arkansas", "ar"),
      Array("California", "ca"),
      Array("Colorado", "co"),
      Array("Connecticut", "ct"),
      Array("Delaware", "de"),
      Array("District of Columbia", "dc"),
      Array("Florida", "fl"),
      Array("Georgia", "ga"),
      Array("Hawaii", "hi"),
      Array("Idaho", "id"),
      Array("Illinois", "il"),
      Array("Indiana", "in"),
      Array("Iowa", "ia"),
      Array("Kansas", "ks"),
      Array("Kentucky", "ky"),
      Array("Louisiana", "la"),
      Array("Maine", "me"),
      Array("Maryland", "md"),
      Array("Massachusetts", "ma"),
      Array("Michigan", "mi"),
      Array("Minnesota", "mn"),
      Array("Mississippi", "ms"),
      Array("Missouri", "mo"),
      Array("Montana", "mt"),
      Array("Nebraska", "ne"),
      Array("Nevada", "nv"),
      Array("New Hampshire", "nh"),
      Array("New Jersey", "nj"),
      Array("New Mexico", "nm"),
      Array("New York", "ny"),
      Array("North_Carolina", "nc"),
      Array("North_Dakota", "nd"),
      Array("Ohio", "oh"),
      Array("Oklahoma", "ok"),
      Array("Oregon", "or"),
      Array("Pennsylvania", "pa"),
      Array("Puerto Rico", "pr"),
      Array("Rhode Island", "ri"),
      Array("South Carolina", "sc"),
      Array("South Dakota", "sd"),
      Array("Tennessee", "tn"),
      Array("Texas", "tx"),
      Array("Utah", "ut"),
      Array("Vermont", "vt"),
      Array("Virginia", "va"),
      Array("Washington", "wa"),
      Array("West Virginia", "wv"),
      Array("Wisconsin", "wi"),
      Array("Wyoming", "wy"),
    )

    var geoheaders = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\tableFiles2\\Geohead.csv") // CHANGE
    geoheaders.createOrReplaceTempView("geoheaderimp")

    var startgeo = spark.sql("SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON FROM geoheaderimp")

    var GeoNames = geoheaders.columns
    var Geostring = GeoNames.mkString(",")
    var Geolist = Geostring.split(",")

    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)
      println(state)
      var df1 = spark.read.option("delimiter", "|").csv(s"D:/Downloads/Work/data/${abbreviation}geo2020.pl")

      var geodfhead = df1.toDF(Geolist: _*)
      geodfhead.createOrReplaceTempView(s"GeoHeader${abbreviation}Imp")

      var geodatahead = spark.sql(s"SELECT STUSAB, SUMLEV, LOGRECNO, GEOID, BASENAME, NAME, POP100, INTPTLAT, INTPTLON" +
        s" FROM GeoHeader${abbreviation}Imp WHERE SUMLEV = 160 AND NOT (NAME LIKE '%CCD%') AND NOT (NAME LIKE '%township%') AND NOT (NAME LIKE '%(part)%')" +
        s" AND NOT (NAME LIKE '%CDP%') AND POP100 > 100000")

     // geodatahead.show(100)                                                                 // MAKE SURE TO CHANGE POP FROM SUM IN TABLEU

      var finalgeo = geodatahead

      startgeo = startgeo.union(finalgeo).distinct()

    }

    println("States Done")

    //J_file.outputcsv("testing", "Geo2020Low", startgeo)
    startgeo.coalesce(1).write.option("header", "true").csv("D:\\Downloads\\Work\\data\\testing\\Geohigh")

    println("Output Done")

    var duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration / 1000000000) + " Seconds")
  }
}
