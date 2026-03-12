package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.slf4j.LoggerFactory

object BronzeTableBootstrap {

  private val logger = LoggerFactory.getLogger(getClass)

  def ensureBronzeTable(
                         spark: SparkSession,
                         inputPath: String,
                         tableName: String,
                         format: String,
                         partitionColumns: Seq[Transform] = Seq()
                       ): Unit = {

    val namespace = "local.bronze"
    val fullTableName = s"$namespace.$tableName"

    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")

    if (!spark.catalog.tableExists(fullTableName)) {

      logger.info(s"Creating Bronze table: $fullTableName")

      val schemaDf: DataFrame =
        try {

          val reader = spark.read.option("inferSchema", "true")

          val df = format match {

            case "csv" =>
              reader
                .option("header", "true")
                .option("sep", ";")
                .csv(inputPath)

            case "parquet" =>
              reader.parquet(inputPath)

            case "json" =>
              reader.json(inputPath)

            case other =>
              throw new IllegalArgumentException(s"Unsupported format: $other")
          }

          df.limit(0)
            .withColumn("ingestion_timestamp", F.lit(null).cast("timestamp"))
            .withColumn("source_file", F.lit(null).cast("string"))

        } catch {

          case ex: Exception =>

            logger.warn(
              s"Could not infer schema from $inputPath. Creating minimal table.",
              ex
            )

            spark.sql(
              """
              SELECT
                CAST(NULL AS TIMESTAMP) AS ingestion_timestamp,
                CAST(NULL AS STRING) AS source_file
              WHERE 1 = 0
              """
            )
        }

      // criar view temporária com schema
      val tempView = s"tmp_${tableName}_schema"
      schemaDf.createOrReplaceTempView(tempView)

      val partitionClause =
        if (partitionColumns.nonEmpty)
          s"PARTITIONED BY (${partitionColumns.mkString(", ")})"
        else
          ""

      spark.sql(
        s"""
        CREATE TABLE $fullTableName
        USING iceberg
        $partitionClause
        AS
        SELECT *
        FROM $tempView
        WHERE 1 = 0
        """
      )

      logger.info(s"Bronze table created successfully: $fullTableName")

    } else {

      logger.info(s"Bronze table already exists: $fullTableName")

    }
  }
}