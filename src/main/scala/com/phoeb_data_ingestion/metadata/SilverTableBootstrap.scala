package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SilverTableBootstrap {

  private val logger = LoggerFactory.getLogger(getClass)

  def ensureEnaSubsystemTable(spark: SparkSession): Unit = {

    logger.info("Creating table silver.enaSubsystem.")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.silver.enaSubsystem (
        |id_subsistema STRING,
        |nom_subsistema STRING,
        |ena_data TIMESTAMP,
        |ena_bruta_regiao_mwmed DOUBLE,
        |ena_bruta_regiao_percentualmlt DOUBLE,
        |ena_armazenavel_regiao_mwmed DOUBLE,
        |ena_armazenavel_regiao_percentualmlt DOUBLE
        |)
        |USING iceberg
        |PARTITIONED BY (id_subsistema, months(ena_data))
        |""".stripMargin)

    logger.info("Table silver.enaSubsystem created successfully.")
  }
}
