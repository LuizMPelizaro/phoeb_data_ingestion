package com.phoeb_data_ingestion.jobs

import scala.util.Try
import java.nio.file.Path

trait IngestionJob {
  def run(): Try[List[Path]]
}