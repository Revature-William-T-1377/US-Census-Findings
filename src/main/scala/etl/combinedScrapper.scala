package etl

import java.io._
import java.net.URL
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object combinedScrapper {

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
      unzip("2020.zip")
      println(counter.toString + "/" + locations.length)
      counter = counter + 1
    }

    val file1Name = "tableFiles/Segment1.csv"
    var fields1 = ""
    for (line <- Source.fromFile(file1Name).getLines()) {
      fields1 = line
    }

    val file2Name = "tableFiles/Segment2.csv"
    var fields2 = ""
    for (line <- Source.fromFile(file2Name).getLines()) {
      fields2 = line
    }

    val file3Name = "tableFiles/Segment3.csv"
    var fields3 = ""
    for (line <- Source.fromFile(file3Name).getLines()) {
      fields3 = line
    }

    val file4Name = "tableFiles/Geohead.csv"
    var fields4 = ""
    for (line <- Source.fromFile(file4Name).getLines()) {
      fields4 = line
    }

    for (states <- locations) {
      val file1Name = s"${states(1)}00001.csv"
      var pl1Name = ""
      if (states(0) == "National") {
        pl1Name = s"${states(1)}000012020.npl"
      } else {
        pl1Name = s"${states(1)}000012020.pl"
      }
      val writer1 = new BufferedWriter(new FileWriter(new File("./datasets/2020/", file1Name), true))
      writer1.write(fields1 + "\n")
      for (lines <- Source.fromFile(pl1Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer1.write(writeLine)
      }
      writer1.close()

      val file2Name = s"${states(1)}00002.csv"
      var pl2Name = ""
      if (states(0) == "National") {
        pl2Name = s"${states(1)}000022020.npl"
      } else {
        pl2Name = s"${states(1)}000022020.pl"
      }
      val writer2 = new BufferedWriter(new FileWriter(new File("./datasets/2020/", file2Name), true))
      writer2.write(fields1 + "\n")
      for (lines <- Source.fromFile(pl2Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer2.write(writeLine)
      }
      writer2.close()

      val file3Name = s"${states(1)}00003.csv"
      var pl3Name = ""
      if (states(0) == "National") {
        pl3Name = s"${states(1)}000032020.npl"
      } else {
        pl3Name = s"${states(1)}000032020.pl"
      }
      val writer3 = new BufferedWriter(new FileWriter(new File("./datasets/2020/", file3Name), true))
      writer3.write(fields3 + "\n")
      for (lines <- Source.fromFile(pl3Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer3.write(writeLine)
      }
      writer3.close()

      val file4Name = s"${states(1)}geo.csv"
      var pl4Name = ""
      if (states(0) == "National") {
        pl4Name = s"${states(1)}geo2020.npl"
      } else {
        pl4Name = s"${states(1)}geo2020.pl"
      }
      val writer4 = new BufferedWriter(new FileWriter(new File("./datasets/2020/", file4Name), true))
      writer4.write(fields4 + "\n")
      for (lines <- Source.fromFile(pl4Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer4.write(writeLine)
      }
      writer4.close()
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

      unzip("1.zip")
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
      val file1Name = s"${states(1)}00001.csv"
      var upl1Name = ""
      if (states(0) == "National") {
        upl1Name = s"${states(1)}000012010.npl"
      } else {
        upl1Name = s"${states(1)}000012010.pl"
      }

      val writer1 = new BufferedWriter(new FileWriter(new File("./datasets/2010/",file1Name), true))
      writer1.write(fields1)
      for (lines <- Source.fromFile(upl1Name).getLines()) {
        val writeLine = lines + "\n"
        writer1.write(writeLine)
      }
      writer1.close()

      val file2Name = s"${states(1)}00002.csv"
      var upl2Name = ""
      if(states(0) == "National") {
        upl2Name = s"${states(1)}000022010.npl"
      } else {
        upl2Name = s"${states(1)}000022010.pl"
      }
      val writer2 = new BufferedWriter(new FileWriter(new File("./datasets/2010/", file2Name), true))
      writer2.write(fields2)
      for (lines <- Source.fromFile(upl2Name).getLines()) {
        val writeLine = lines + "\n"
        writer2.write(writeLine)
      }
      writer2.close()

      val csvName = s"${states(1)}geo.csv"
      var uplName = ""
      if(states(0) == "National") {
        uplName = s"${states(1)}geo2010.npl"
      } else {
        uplName = s"${states(1)}geo2010.pl"
      }
      val geoWriter = new BufferedWriter(new FileWriter(new File("./datasets/2010/", csvName), true))
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
        geoUrl = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/usgeo.upl.zip"
        url1 = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/us00001.upl.zip"
        url2 = "https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/0US_Summary/us00002.upl.zip"
      } else {
        geoUrl = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}geo.upl.zip"
        url1 = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}00001.upl.zip"
        url2 = s"https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/${state}/${abbreviation}00002.upl.zip"
      }

      fileDownload(geoUrl, "geo.zip")
      fileDownload(url1, "1.zip")
      fileDownload(url2, "2.zip")

      unzip("geo.zip")
      unzip("1.zip")
      unzip("2.zip")

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

    //fills in 00001.csv and 00002.csv with field names and data from upl files
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

      val file2Name = s"${states(1)}00002.csv"
      val upl2Name = s"${states(1)}00002.upl"
      val writer2 = new BufferedWriter(new FileWriter(new File("datasets/2000/", file2Name), true))
      writer2.write(fields2)
      for (lines <- Source.fromFile(upl2Name).getLines()) {
        val writeLine = lines + "\n"
        writer2.write(writeLine)
      }
      writer2.close()
    }

    //sets up the headers for geo files
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

    //iterates through
    for (states <- locations) {
      val csvName = s"${states(1)}geo.csv"
      val uplName = s"${states(1)}geo.upl"
      val geoWriter = new BufferedWriter(new FileWriter(new File("datasets/2000/", csvName), true))
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
  } // finished

  def run(): Unit = {
    Files.createDirectories(Paths.get("./datasets/2010/"))
    Files.createDirectories(Paths.get("./datasets/2000/"))
    Files.createDirectories(Paths.get("./datasets/2020/"))
    val t1 = System.nanoTime()
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
