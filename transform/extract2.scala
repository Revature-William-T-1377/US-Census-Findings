import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.{File, FileInputStream, FileOutputStream, FileWriter, InputStream, PrintWriter}
import java.util.zip.ZipInputStream
import scala.io.Source
import util.Try

object extract2 {
  val t1 = System.nanoTime
  var locations:Array[Array[String]] = _

  //Downloads a file given some url and file name
  def fileDownload(url: String, fileName: String) = {
    new URL(url) #> new File(fileName) !!
  }

  //Unzips zip files
  def unzip(fileName: String) = {
    val fInStream = new FileInputStream(fileName)
    val zInString = new ZipInputStream(fInStream)
    Stream.continually(zInString.getNextEntry).takeWhile(_ != null).foreach{ file =>
      val fout = new FileOutputStream(file.getName)
      val buffer = new Array[Byte](1024)
      Stream.continually(zInString.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))
    }
  }

  def main(args: Array[String]): Unit = {

    //2D array with all folders and abbreviations for 2010 census data
    locations = Array( Array("Alabama", "al"),
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
    run2010()
  }

  def run2010():Unit ={

    //downloads zip files from 2000 census data, unzips files
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)

      val url1 = s"https://www2.census.gov/census_2010/redistricting_file--pl_94-171/${state}/${abbreviation}2010.pl.zip"

      fileDownload(url1, "1.zip")

      unzip("1.zip")
    }

    //sets up field names for 00001.csv and 00002.csv
    val file1Name = "tableFiles/PL_PART1.csv"
    var fields1 = ""
    for (line <- Source.fromFile(file1Name).getLines()) {
      for (lines <- line.split("\"")) {
        if (lines != ",") {
          fields1 += lines.split(",")(0) + ","
        }
      }
    }
    fields1 = fields1.substring(1, fields1.length()-1) + "\n"


    val file2Name = "tableFiles/PL_PART2.csv"
    var fields2 = ""
    for (line <- Source.fromFile(file2Name).getLines()) {
      for (lines <- line.split("\"")) {
        if (lines != ",") {
          fields2 += lines.split(",")(0) + ","
        }
      }
    }
    fields2 = fields2.substring(1, fields2.length()-1) + "\n"

    val fileName = "tableFiles/0HEADER.csv"
    var geoHeaders = ""
    var lengths = Array[Int]()
    var firstFlag = true
    for (line <- Source.fromFile(fileName).getLines()) {
      val splitLine = line.split(",")
      if (!firstFlag) {
        geoHeaders += splitLine(1) + ","
        lengths = lengths :+ splitLine(2).toFloat.toInt
      } else {
        firstFlag = false
      }
    }
    geoHeaders = geoHeaders.substring(0, geoHeaders.length()-1) + "\n"

    //fills in 00001.csv and 00002.csv with field names and data from upl files
    for (states <- locations) {
      val file1Name = s"${states(1)}000012010.csv"
      val upl1Name = s"${states(1)}000012010.pl"
      val writer1 = new FileWriter(file1Name, true)
      writer1.write(fields1)
      for (lines <- Source.fromFile(upl1Name).getLines()) {
        val writeLine = lines + "\n"
        writer1.write(writeLine)
      }
      writer1.close()

      val file2Name = s"${states(1)}000022010.csv"
      val upl2Name = s"${states(1)}000022010.pl"
      val writer2 = new FileWriter(file2Name, true)
      writer2.write(fields2)
      for (lines <- Source.fromFile(upl2Name).getLines()) {
        val writeLine = lines + "\n"
        writer2.write(writeLine)
      }
      writer2.close()


      val csvName = s"${states(1)}geo2010.csv"
      val uplName = s"${states(1)}geo2010.pl"
      val geoWriter = new FileWriter(csvName, true)
      geoWriter.write(geoHeaders)
      for (lines <- Source.fromFile(uplName)("ISO-8859-1").getLines()) {
        var splittingLine = lines
        var finalLine = ""
        for (i <- lengths) {
          finalLine += splittingLine.substring(0, i) + ","
          splittingLine = splittingLine.substring(i, splittingLine.length())
        }
        finalLine += "\n"
        geoWriter.write(finalLine)
      }
      geoWriter.close()

    }
    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration/1000000000) + " Seconds")
  }
}