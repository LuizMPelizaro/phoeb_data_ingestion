package com.phoeb_data_ingestion.service

import com.phoeb_data_ingestion.metadata.ProcessedFilesRepository
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class FileTrackerService(
                          spark: SparkSession,
                          repository: ProcessedFilesRepository
                        ) {

  private val logger = LoggerFactory.getLogger(getClass)

  def listNewFiles(inputPath: String, jobName: String): Seq[String] = {

    logger.info(s"Scanning files for job: $jobName")
    logger.info(s"Input path: $inputPath")

    val allFiles = spark.read
      .format("binaryFile")
      .load(inputPath)
      .select("path")
      .collect()
      .map(_.getString(0))
      .toSeq

    val processed = repository.getProcessedFiles(jobName)

    val newFiles = allFiles.diff(processed)

    logger.info(s"Total files found: ${allFiles.size}")
    logger.info(s"Already processed files: ${processed.size}")
    logger.info(s"New files detected: ${newFiles.size}")

    if (newFiles.nonEmpty) {
      logger.info(s"Files to process: ${newFiles.mkString(", ")}")
    } else {
      logger.info("No new files to process.")
    }

    newFiles
  }
}