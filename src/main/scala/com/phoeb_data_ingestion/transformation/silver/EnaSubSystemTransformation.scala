package com.phoeb_data_ingestion.transformation.silver

import com.phoeb_data_ingestion.domain.silver.EnaSubSystems
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.slf4j.LoggerFactory

object EnaSubSystemTransformation {

  private val logger = LoggerFactory.getLogger(getClass)

  def transform(bronzeDf: DataFrame)
               (implicit spark: SparkSession): Dataset[EnaSubSystems] = {
    import spark.implicits._

    logger.info("Starting transformation from Bronze -> Silver")

    val transformedDf =
      bronzeDf.drop("ingestion_timestamp", "source_file")
        .withColumn("id_subsistema", F.col("id_subsistema").cast("string"))
        .withColumn("nom_subsistema", F.col("nom_subsistema").cast("string"))
        .withColumn("ena_data", F.col("ena_data").cast("timestamp"))
        .withColumn("ena_bruta_regiao_mwmed", F.col("ena_bruta_regiao_mwmed").cast("double"))
        .withColumn("ena_bruta_regiao_percentualmlt", F.col("ena_bruta_regiao_percentualmlt").cast("double"))
        .withColumn("ena_armazenavel_regiao_mwmed", F.col("ena_armazenavel_regiao_mwmed").cast("double"))
        .withColumn("ena_armazenavel_regiao_percentualmlt", F.col("ena_armazenavel_regiao_percentualmlt").cast("double"))

    logger.info("Casting columns to target Silver types")

    val dataset = transformedDf.as[EnaSubSystems]

    logger.info("Transformation completed successfully")

    dataset
  }

}
