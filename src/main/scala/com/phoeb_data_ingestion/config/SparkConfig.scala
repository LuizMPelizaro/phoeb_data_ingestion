package com.phoeb_data_ingestion.config

case class SparkConfig(
  appName: String,
  master: Option[String],
  configs : Map[String, String],
  enableHive: Boolean = false
)
