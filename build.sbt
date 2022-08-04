ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "US-Census-Findings"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.0"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.12.239"
)

libraryDependencies += "com.lihaoyi" %% "ujson" % "2.0.0"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.8.1"

libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.1"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
libraryDependencies +="org.jsoup" % "jsoup" % "1.14.3"

libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.8.1"

libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % "3.3.2")
libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-aws" % "3.3.2")
libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-common" % "3.3.2")
libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-auth" % "3.3.2")