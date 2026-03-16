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

  def ensuringLoadTable(spark: SparkSession): Unit = {
    logger.info("Creating table silver.Load")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.silver.load(
        |id_subsistema STRING,
        |nom_subsistema STRING,
        |din_instante TIMESTAMP,
        |val_cargaenergiahomwmed DOUBLE
        |)
        |USING iceberg
        |PARTITIONED BY (id_subsistema, months(din_instante))
        |""".stripMargin
    )
    logger.info("Table silver.load created successfully.")
  }

  def ensuringGenerateTable(spark: SparkSession): Unit = {
    logger.info("Creating table silver.generate")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS local.silver.generate(
        |din_instante TIMESTAMP,
        |id_subsistema STRING,
        |nom_subsistema  STRING,
        |id_estado STRING,
        |nom_estado STRING,
        |cod_modalidadeoperacao STRING,
        |nom_tipousina STRING,
        |nom_tipocombustivel STRING,
        |nom_usina STRING,
        |id_ons STRING,
        |ceg STRING,
        |val_geracao DOUBLE
        |)
        |USING iceberg
        |PARTITIONED BY (id_subsistema,id_estado, months(din_instante))
        |""".stripMargin
    )
    logger.info("Table silver.generate created successfully.")
  }
}
