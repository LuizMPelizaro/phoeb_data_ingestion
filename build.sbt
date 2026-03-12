ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "phoeb_data_ingestion",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1",
    libraryDependencies += "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
    libraryDependencies += "io.github.cdimascio" % "dotenv-java" % "3.2.0",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.17" ,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.32",
    libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.10.1"
  )

Compile / run / javaOptions ++= Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)