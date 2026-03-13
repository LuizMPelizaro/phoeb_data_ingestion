package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.{SparkSession, functions => F}
import org.slf4j.LoggerFactory

import java.sql.Timestamp

class WatermarkRepository(spark: SparkSession) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val table = "local.metadata.job_watermark"

  def getLastProcessed(jobName: String): Option[Timestamp] = {

    logger.info(s"Fetching last processed watermark for job: $jobName")

    val rows =
      spark.table(table)
        .filter(F.col("job_name") === jobName)
        .select("last_processed_timestamp")
        .take(1)

    if (rows.isEmpty) {

      logger.info("No watermark found. Full load will be executed.")
      None

    } else {

      val ts = rows(0).getTimestamp(0)

      logger.info(s"Found watermark: $ts")

      Some(ts)

    }
  }

  def update(jobName: String, tableName: String, timestamp: String): Unit = {

    logger.info(s"Updating watermark for job: $jobName")

    spark.sql(
      s"""
      MERGE INTO $table t
      USING (
        SELECT
          '$jobName' as job_name,
          '$tableName' as table_name,
          timestamp('$timestamp') as last_processed_timestamp,
          current_timestamp() as updated_at
      ) s
      ON t.job_name = s.job_name

      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
      """
    )

    logger.info(s"Watermark updated to: $timestamp")

  }

}