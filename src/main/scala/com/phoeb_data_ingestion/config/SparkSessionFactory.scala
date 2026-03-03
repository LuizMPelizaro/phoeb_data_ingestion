package com.phoeb_data_ingestion.config

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  def create(config: SparkConfig): SparkSession = {
    logger.info(s"Starting Spark Session for app: ${config.appName}")

    val builder = SparkSession.builder()
      .appName(config.appName)

    config.master.foreach{m =>
      logger.info(s"Using master: $m")
      builder.master(m)
    }

    config.configs.foreach{ case(k,v)=>
      logger.debug(s"Applying Spark config: $k=$v")
      builder.config(k,v)
    }

    if (config.enableHive){
      logger.info("Enabling Hive Support")
      builder.enableHiveSupport()
    }
    val spark = builder.getOrCreate()

    logger.info("Spark Session started successfully")

    spark
  }
}
