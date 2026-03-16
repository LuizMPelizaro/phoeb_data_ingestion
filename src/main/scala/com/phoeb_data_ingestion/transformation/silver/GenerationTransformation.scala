package com.phoeb_data_ingestion.transformation.silver

import com.phoeb_data_ingestion.domain.silver.Generation
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.slf4j.LoggerFactory

object GenerationTransformation {

  private val logger = LoggerFactory.getLogger(getClass)

  def transform(bronzeDf: DataFrame)(implicit spark: SparkSession): Dataset[Generation] = {
    import spark.implicits._

    logger.info("Starting transformation from Bronze -> Silver")

    val transformedDf =
      bronzeDf.drop("ingestion_timestamp", "source_file")
        .withColumn("din_instante", F.col("din_instante").cast("timestamp"))
        .withColumn("id_subsistema", F.col("id_subsistema").cast("string"))
        .withColumn("nom_subsistema", F.col("nom_subsistema").cast("string"))
        .withColumn("id_estado", F.col("id_estado").cast("string"))
        .withColumn("nom_estado", F.col("nom_estado").cast("string"))
        .withColumn("cod_modalidadeoperacao", F.col("cod_modalidadeoperacao").cast("string"))
        .withColumn("nom_tipousina", F.col("nom_tipousina").cast("string"))
        .withColumn("nom_tipocombustivel", F.col("nom_tipocombustivel").cast("string"))
        .withColumn("nom_usina", F.col("nom_usina").cast("string"))
        .withColumn("id_ons", F.col("id_ons").cast("string"))
        .withColumn("ceg", F.col("ceg").cast("string"))
        .withColumn("val_geracao", F.col("val_geracao").cast("Double"))

    logger.info("Casting columns to target Silver types")

    val dataset = transformedDf.as[Generation]

    dataset
  }
}
