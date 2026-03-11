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

    val icebergWarehouse =
      Option(dotenv.get("ICEBERG_WAREHOUSE"))
        .getOrElse("./data/iceberg_warehouse")

    val baseConfigs = Map(
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.shuffle.partitions" ->
        Option(dotenv.get("SPARK_SQL_SHUFFLE_PARTITIONS")).getOrElse("200"),

      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",

      "spark.driver.memory" -> "8g",
      "spark.executor.memory" -> "8g",
      "spark.sql.shuffle.partitions" -> "50",
      "spark.sql.files.maxPartitionBytes" -> "134217728",

      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.local" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.local.type" -> "hadoop",
      "spark.sql.catalog.local.warehouse" -> icebergWarehouse
    )

    SparkConfig(
      appName = "phoeb-data-ingestion",
      master = master,
      configs = baseConfigs,
      enableHive = environment != "local"
    )
  }
}
