package com.phoeb_data_ingestion.jobs.silver

import com.phoeb_data_ingestion.jobs.SparkJob
import com.phoeb_data_ingestion.metadata.{SilverTableBootstrap, WatermarkRepository}
import com.phoeb_data_ingestion.transformation.silver.GenerationTransformation
import com.phoeb_data_ingestion.validation.DataQualityReport
import org.apache.spark.sql.{SparkSession, functions => F}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class SilverGenerateJob(spark: SparkSession, tableName: String) extends SparkJob {

  private val logger = LoggerFactory.getLogger(getClass)

  private val jobName = "silver_generate"

  private val watermarkRepo = new WatermarkRepository(spark)

  override def run(): Try[Unit] = {
    logger.info(s"Start Silver Job $jobName")

    val result = Try {
      if (!spark.catalog.tableExists(s"local.silver.$tableName")) {
        SilverTableBootstrap.ensuringGenerateTable(spark)
      }

      val lastWatermark = watermarkRepo.getLastProcessed(jobName)

      val bronzeDf = spark.table(s"local.bronze.$tableName")

      logger.info("Loaded Bronze table")

      val incrementalDf =
        lastWatermark match {
          case Some(ts) =>
            logger.info(s"Filtering Bronze data after watermark: $ts")
            bronzeDf.filter(F.col("din_instante") > ts)

          case None =>
            logger.info("Executing full load")
            bronzeDf
        }

      if (incrementalDf.isEmpty) {
        logger.info("No new data to process")
        return Success(())
      }

      logger.info("Starting bronze -> Silver Transformation")

      implicit val sparkSession: SparkSession = spark

      val silverDf = GenerationTransformation.transform(incrementalDf)

      logger.info("Transformation completed")

      logger.info("Running Data quality checks")

      val qualityReport = DataQualityReport.generate(silverDf.toDF)

      qualityReport.show(false)

      logger.info("Writing data to Silver table")

      silverDf
        .writeTo("local.silver.generate")
        .append()

      val maxTimestamp =
        silverDf
          .toDF
          .agg(F.max("din_instante"))
          .head()
          .getTimestamp(0)

      logger.info(s"Max processed timestamp: $maxTimestamp")

      watermarkRepo.update(
        jobName,
        "generate",
        maxTimestamp.toString
      )
      logger.info("Watermark update")

    }

    result match {

      case Success(_) =>
        logger.info(s"Silver job $jobName finished successfully")

      case Failure(ex) =>
        logger.error(s"Silver job $jobName failed", ex)

    }

    result
  }

}
