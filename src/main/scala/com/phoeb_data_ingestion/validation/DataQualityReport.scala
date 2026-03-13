package com.phoeb_data_ingestion.validation

import org.apache.spark.sql.{DataFrame, functions => F}
import org.slf4j.LoggerFactory

object DataQualityReport {

  private val logger = LoggerFactory.getLogger(getClass)

  def generate(df: DataFrame): DataFrame = {

    logger.info("Generating Data Quality Report")

    val cols = df.columns

    val metrics = cols.map { c =>

      logger.debug(s"Calculating metrics for column: $c")

      df.select(
        F.lit(c).alias("column"),
        F.count(F.when(F.col(c).isNull, 1)).alias("nulls"),
        F.count("*").alias("total_rows")
      )
    }

    val report = metrics.reduce(_ union _)

    logger.info("Data Quality Report generated")

    report
  }

}