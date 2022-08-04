import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import java.io.File
import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.{File, FileInputStream, FileOutputStream, FileWriter, InputStream, PrintWriter}
import java.util.zip.ZipInputStream


object unzipping {
  def main(args: Array[String]): Unit = {
    var t1 = System.nanoTime

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")


    //Downloads a file given some url and file name
    def fileDownload(url: String, fileName: String) = {
      new URL(url) #> new File(fileName) !!
    }

    //Unzips zip files
    def unzip(fileName: String) = {
      val fInStream = new FileInputStream(fileName)
      val zInString = new ZipInputStream(fInStream)
      Stream.continually(zInString.getNextEntry).takeWhile(_ != null).foreach { file =>
        val fout = new FileOutputStream(file.getName)
        val buffer = new Array[Byte](1024)
        Stream.continually(zInString.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
      }
    }

    //2D array with all folders and abbreviations for 2000 census data
    val locations = Array(
      Array("Alabama", "al"),
      Array("Alaska", "ak"),
      Array("Arizona", "az"),
      Array("Arkansas", "ar"),
      Array("California", "ca"),
      Array("Colorado", "co"),
      Array("Connecticut", "ct"),
      Array("Delaware", "de"),
      Array("District_of_Columbia", "dc"),
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
      Array("New_Hampshire", "nh"),
      Array("New_Jersey", "nj"),
      Array("New_Mexico", "nm"),
      Array("New_York", "ny"),
      Array("North_Carolina", "nc"),
      Array("North_Dakota", "nd"),
      Array("Ohio", "oh"),
      Array("Oklahoma", "ok"),
      Array("Oregon", "or"),
      Array("Pennsylvania", "pa"),
      Array("Puerto_Rico", "pr"),
      Array("Rhode_Island", "ri"),
      Array("South_Carolina", "sc"),
      Array("South_Dakota", "sd"),
      Array("Tennessee", "tn"),
      Array("Texas", "tx"),
      Array("Utah", "ut"),
      Array("Vermont", "vt"),
      Array("Virginia", "va"),
      Array("Washington", "wa"),
      Array("West_Virginia", "wv"),
      Array("Wisconsin", "wi"),
      Array("Wyoming", "wy"),
    )
// ---------------------------------------------------Setup Done----------------------------------------------------------
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)
      val url1 = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}00001.upl.zip"

      fileDownload(url1,"1.zip")

      unzip("1.zip")
    }

    val headers = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\FinalExport\\ObtainedETLDataV1\\HeaderX.csv") // File location in hdfs
    headers.createOrReplaceTempView("HeaderImp")

    var HeaderNames = headers.columns
    var Headerstring = HeaderNames.mkString(",")
    var Headerlist = Headerstring.split(",")

    for (i <- locations) {
      val abbreviation = i(1)

    val df = spark.read.option("delimiter", ",").csv(s"D:\\Downloads\\Work\\data\\${abbreviation}00001.upl") //

    val dfhead = df.toDF(Headerlist: _*)

    file.outputcsv("2020", s"${abbreviation}000012000", dfhead)
  }










    var duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration / 1000000000) + " Seconds")
  }
}
