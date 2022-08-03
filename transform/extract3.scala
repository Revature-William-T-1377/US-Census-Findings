import scala.language.postfixOps
import sys.process._
import java.net.URL
import java.io.{BufferedWriter, File, FileInputStream, FileOutputStream, FileWriter, InputStream, PrintWriter}
import java.util.zip.ZipInputStream
import scala.io.Source
import util.Try

object extract3 {
  val t1 = System.nanoTime
  //Downloads a file given some url and file name
  def fileDownload(url: String, fileName: String) = {
    new URL(url) #> new File("CSVs3/",fileName) !!
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


    var counter = 1
    //downloads zip files from 2000 census data, unzips files
    for (i <- locations) {
      val state = i(0)
      val abbreviation = i(1)
      var url = ""
      if(i(0) == "National") {
        url = s"https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/National/us2020.npl.zip"
      } else {
        url = s"https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/${state}/${abbreviation}2020.pl.zip"
      }
      fileDownload(url, "2020.zip")
      unzip("CSVs3/2020.zip")
      println(counter.toString + "/" + locations.length)
      counter = counter + 1
    }

    //sets up field names for 00001.csv and 00002.csv
    val file1Name = "tableFiles2/Segment1.csv"
    var fields1 = ""
    for (line <- Source.fromFile(file1Name).getLines()) {
      fields1 = line
    }

    val file2Name = "tableFiles2/Segment2.csv"
    var fields2 = ""
    for (line <- Source.fromFile(file2Name).getLines()) {
      fields2 = line
    }

    val file3Name = "tableFiles2/Segment3.csv"
    var fields3 = ""
    for (line <- Source.fromFile(file3Name).getLines()) {
      fields3 = line
    }

    val file4Name = "tableFiles2/Geohead.csv"
    var fields4 = ""
    for (line <- Source.fromFile(file4Name).getLines()) {
      fields4 = line
    }

    //fills in 00001.csv and 00002.csv with field names and data from upl files
    for (states <- locations) {
      val file1Name = s"${states(1)}00001.csv"
      var pl1Name = ""
      if(states(0) == "National") {
        pl1Name = s"${states(1)}000012020.npl"
      } else {
        pl1Name = s"${states(1)}000012020.pl"
      }
      val writer1 = new BufferedWriter(new FileWriter(new File("CSVs3/",file1Name), true))
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
      if(states(0) == "National") {
        pl2Name = s"${states(1)}000022020.npl"
      } else {
        pl2Name = s"${states(1)}000022020.pl"
      }
      val writer2 = new BufferedWriter(new FileWriter(new File("CSVs3/",file2Name), true))
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
      if(states(0) == "National") {
        pl3Name = s"${states(1)}000032020.npl"
      } else {
        pl3Name = s"${states(1)}000032020.pl"
      }
      val writer3 = new BufferedWriter(new FileWriter(new File("CSVs3/",file3Name), true))
      writer3.write(fields3 + "\n")
      for (lines <- Source.fromFile(pl3Name)("iso-8859-1").getLines()) {
        val tmp = lines
        val prefixed = tmp.replace(",", "")
        val fixed = prefixed.replace("|", ",")
        val writeLine = fixed + "\n"
        writer3.write(writeLine)
      }
      writer3.close()

      val file4Name = s"${states(1)}geo2020.csv"
      var pl4Name = ""
      if(states(0) == "National") {
        pl4Name = s"${states(1)}geo2020.npl"
      } else {
        pl4Name = s"${states(1)}geo2020.pl"
      }
      val writer4 = new BufferedWriter(new FileWriter(new File("CSVs3/",file4Name), true))
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
    val duration = (System.nanoTime - t1)
    println("Code Lasted: " + (duration/1000000000) + " Seconds")
  }
}