package etl

import java.io._
import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object optimizedScraper {

  def fileDownload(url: String, fileName: String) = {
    new URL(url) #> new File("datasets/", fileName) !!
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

  def deleteExtraFiles(): Unit = {
    val plFiles = new File("../US-Census-Findings").listFiles.filter(_.getName.endsWith(".pl"))
    val uplFiles = new File("../US-Census-Findings").listFiles.filter(_.getName.endsWith(".upl"))
    val txtFiles = new File("../US-Census-Findings").listFiles.filter(_.getName.endsWith(".txt"))
    val nplFiles = new File("../US-Census-Findings").listFiles.filter(_.getName.endsWith(".npl"))

    val allFiles = plFiles ++ uplFiles ++ txtFiles ++ nplFiles

    for (file <- allFiles) {
      file.delete()
    }
  }

  def extract2020(locations: Array[Array[String]]): Unit = {
    var counter = 1
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)
      var url = ""
      if (i(0) == "National") {
        url = s"https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/National/us2020.npl.zip"
      } else {
        url = s"https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/${state}/${abbreviation}2020.pl.zip"
      }

      fileDownload(url, "2020.zip")
      unzip("datasets/2020.zip")
      println(counter.toString + "/" + locations.length)
      counter = counter + 1
    }

    val file1Name = "tableFiles/Segment1.csv"
    var fields1 = ""
    for (line <- Source.fromFile(file1Name).getLines()) {
      fields1 = line
    }

    for (states <- locations) {
      val file1Name = s"${states(1)}00001.csv"
      var pl1Name = ""
      if (states(0) == "National") {
        pl1Name = s"${states(1)}000012020.npl"
      } else {
        pl1Name = s"${states(1)}000012020.pl"
      }
      val writer1 = new BufferedWriter(new FileWriter(new File("datasets/2020/", file1Name), true))
      writer1.write(fields1 + "\n")
      for (lines <- Source.fromFile(pl1Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer1.write(writeLine)
      }
      writer1.close()
    }
  } // finished

  def extract2010(locations: Array[Array[String]]): Unit = {
    var counter = 1
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)

      var url1 = ""
      if (i(0) == "National") {
        url1 = "https://www2.census.gov/census_2010/redistricting_file--pl_94-171/National/us2010.npl.zip"
      } else {
        url1 = s"https://www2.census.gov/census_2010/redistricting_file--pl_94-171/${state}/${abbreviation}2010.pl.zip"
      }

      fileDownload(url1, "1.zip")

      unzip("datasets/1.zip")
      println(counter.toString + "/" + locations.length)
      counter = counter + 1
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

    for (states <- locations) {
      val file1Name = s"${states(1)}00001.csv"
      var upl1Name = ""
      if (states(0) == "National") {
        upl1Name = s"${states(1)}000012010.npl"
      } else {
        upl1Name = s"${states(1)}000012010.pl"
      }
      val writer1 = new BufferedWriter(new FileWriter(new File("datasets/2010/",file1Name), true))
      writer1.write(fields1)
      for (lines <- Source.fromFile(upl1Name).getLines()) {
        val writeLine = lines + "\n"
        writer1.write(writeLine)
      }
      writer1.close()
    }
  } // finished

  def extract2000(locations: Array[Array[String]]): Unit = {
    var counter = 1
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)
      var geoUrl = ""
      var url1 = ""
      var url2 = ""
      if(i(0) == "National") {
        //geoUrl = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/usgeo.upl.zip"
        url1 = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/us00001.upl.zip"
        //url2 = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/us00002.upl.zip"
      } else {
        //geoUrl = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}geo.upl.zip"
        url1 = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}00001.upl.zip"
        //url2 = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}00002.upl.zip"
      }
      fileDownload(url1, "1.zip")

      unzip("datasets/1.zip")

      println(counter.toString + "/" + locations.length)
      counter = counter + 1
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

    for (states <- locations) {
      val file1Name = s"${states(1)}00001.csv"
      val upl1Name = s"${states(1)}00001.upl"
      val writer1 = new BufferedWriter(new FileWriter(new File("datasets/2000/", file1Name), true))
      writer1.write(fields1)
      for (lines <- Source.fromFile(upl1Name).getLines()) {
        val writeLine = lines + "\n"
        writer1.write(writeLine)
      }
      writer1.close()
    }

  } // finished

  def run(): Unit = {
    val t1 = System.nanoTime()

    Files.createDirectories(Paths.get("./datasets/2010/"))
    Files.createDirectories(Paths.get("./datasets/2000/"))
    Files.createDirectories(Paths.get("./datasets/2020/"))
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
      Array("National", "us"),
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


    extract2020(locations)
    extract2010(locations)
    extract2000(locations)

    val duration = (System.nanoTime - t1)
    println("Code took " + (duration/1000000000) + " Seconds")
  }
}
