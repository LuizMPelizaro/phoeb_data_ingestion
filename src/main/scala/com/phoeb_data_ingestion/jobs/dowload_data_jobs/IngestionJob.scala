package com.phoeb_data_ingestion.jobs.dowload_data_jobs

import java.nio.file.Path
import scala.util.Try

trait IngestionJob {
  def run(): Try[List[Path]]
}