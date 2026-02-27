ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "phoeb_data_ingestion",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.1.1",
    libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
    libraryDependencies += "io.github.cdimascio" % "dotenv-java" % "3.2.0",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.17" ,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.32"
  )