package com.phoeb_data_ingestion.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object SchemaUtils {

  def alignToTableSchema(
                          spark: SparkSession,
                          df: DataFrame,
                          tableName: String
                        ): DataFrame = {

    val tableSchema = spark.table(tableName).schema

    val alignedColumns = tableSchema.fields.map { field =>

      if (df.columns.contains(field.name)) {
        col(field.name).cast(field.dataType).alias(field.name)
      } else {
        col(field.name)
      }

    }

    df.select(alignedColumns: _*)
  }
}