package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TableBootstrap {

  private val logger = LoggerFactory.getLogger(getClass)

  def ensureMetadataTables(spark: SparkSession): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.metadata")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.metadata.job_runs (
        |  job_name STRING,
        |  start_time TIMESTAMP,
        |  end_time TIMESTAMP,
        |  status STRING
        |) USING iceberg
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.metadata.processed_files (
        |  job_name STRING,
        |  file_path STRING,
        |  ingestion_time TIMESTAMP,
        |  status STRING
        |) USING iceberg
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.metadata.job_watermark (
        | job_name STRING,
        | table_name STRING,
        | last_processed_timestamp TIMESTAMP,
        | current_timestamp TIMESTAMP
        |) USING iceberg
        |""".stripMargin
    )

    logger.info("Metadata tables checked/created: local.metadata.job_runs, local.metadata.processed_files")
  }
}
