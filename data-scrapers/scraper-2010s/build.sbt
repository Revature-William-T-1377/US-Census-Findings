ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "s3ex"
  )

libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.1"
libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.4.2"
libraryDependencies +="org.jsoup" % "jsoup" % "1.14.3"