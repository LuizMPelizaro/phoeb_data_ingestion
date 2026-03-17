package com.phoeb_data_ingestion.transformation.silver

import com.phoeb_data_ingestion.domain.silver.Load
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.slf4j.LoggerFactory

object LoadTransformation {
  private val logger = LoggerFactory.getLogger(getClass)

  def transform(bronzeDf: DataFrame)
               (implicit spark: SparkSession): Dataset[Load] = {
    import spark.implicits._

    logger.info("Starting transformation from Bronze -> Silver")

    val transformedDf =
      bronzeDf.drop("ingestion_timestamp", "source_file")
        .withColumn("id_subsistema", F.col("id_subsistema").cast("string"))
        .withColumn("nom_subsistema", F.col("nom_subsistema").cast("string"))
        .withColumn("din_instante", F.col("din_instante").cast("timestamp"))
        .withColumn("val_cargaenergiahomwmed", F.col("val_cargaenergiahomwmed").cast("double"))

    logger.info("Casting columns to target Silver types")

    val dataset = transformedDf.as[Load]

    logger.info("Transformation completed successfully")

    dataset

  }
}
