package com.phoeb_data_ingestion.jobs.bronze

import com.phoeb_data_ingestion.jobs.SparkJob
import com.phoeb_data_ingestion.metadata.{BronzeTableBootstrap, JobRunRepository, ProcessedFilesRepository}
import com.phoeb_data_ingestion.service.FileTrackerService
import com.phoeb_data_ingestion.utils.SchemaUtils
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.{SparkSession, functions => F}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class BronzeEnaSubSystemsJob(
                           spark: SparkSession,
                           inputPath: String,
                           tableName: String
                         ) extends SparkJob {

  private val logger = LoggerFactory.getLogger(getClass)
  private val jobName = s"bronze_$tableName"

  private val processedRepo = new ProcessedFilesRepository(spark)
  private val jobRunRepo    = new JobRunRepository(spark)
  private val fileTracker   = new FileTrackerService(spark, processedRepo)

  override def run(): Try[Unit] = {

    logger.info(s"Starting Bronze Job: $jobName")

    jobRunRepo.startRun(jobName)

    val result = Try {

      // 1. Garantir que tabela Bronze existe
      BronzeTableBootstrap.ensureBronzeTable(
        spark = spark,
        inputPath = inputPath,
        tableName = tableName,
        format = "csv",
        partitionColumns = Seq(
          Expressions.identity("id_subsistema"),
          Expressions.months("ena_data")
        )
      )

      val newFiles = fileTracker.listNewFiles(inputPath, jobName)

      if (newFiles.isEmpty) {
        logger.info("No new files found to process.")
        return Success(())
      }

      logger.info(s"Found ${newFiles.size} new files to process.")

      val rawDf = spark.read
        .option("header", "true")
        .option("sep", ";")
        .csv(newFiles: _*)

      logger.info(s"Total rows read: ${rawDf.count()}")

      val enrichedDf = rawDf
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())

      val alignedDf = SchemaUtils.alignToTableSchema(
        spark,
        enrichedDf,
        s"local.bronze.$tableName"
      )

      alignedDf.writeTo(s"local.bronze.$tableName").append()

      logger.info(s"Data appended successfully into local.bronze.$tableName")

      processedRepo.saveSuccess(jobName, newFiles)

      logger.info("Processed files metadata saved.")
    }

    result match {
      case Success(_) =>
        jobRunRepo.finishRun(jobName, "SUCCESS")
        logger.info(s"Job $jobName finished successfully.")

      case Failure(ex) =>
        jobRunRepo.finishRun(jobName, "FAILED")
        logger.error(s"Job $jobName failed.", ex)
    }

    result
  }
}