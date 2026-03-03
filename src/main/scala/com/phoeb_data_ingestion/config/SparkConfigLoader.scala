package com.phoeb_data_ingestion.config

import io.github.cdimascio.dotenv.Dotenv

object SparkConfigLoader {

  private val dotenv = Dotenv.configure().ignoreIfMissing().load()

  def load(): SparkConfig = {

    val environment =
      Option(dotenv.get("ENV")).getOrElse("local")

    val master =
      if (environment == "local")
        Some(Option(dotenv.get("SPARK_MASTER")).getOrElse("local[*]"))
      else None

    val baseConfigs = Map(
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.shuffle.partitions" ->
        Option(dotenv.get("SPARK_SQL_SHUFFLE_PARTITIONS"))
          .getOrElse("200"),
      "spark.serializer" ->
        "org.apache.spark.serializer.KryoSerializer"
    )

    SparkConfig(
      appName = "phoeb-data-ingestion",
      master = master,
      configs = baseConfigs,
      enableHive = environment != "local"
    )
  }
}
