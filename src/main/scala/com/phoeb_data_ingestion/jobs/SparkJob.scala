package com.phoeb_data_ingestion.jobs

import scala.util.Try

trait SparkJob {
  def run(): Try[Unit]
}
