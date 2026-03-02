package com.phoeb_data_ingestion.config

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(app_name: String) : SparkSession =
    SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()

}
