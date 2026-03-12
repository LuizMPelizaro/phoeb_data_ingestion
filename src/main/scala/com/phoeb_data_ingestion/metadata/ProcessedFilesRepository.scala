package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class ProcessedFilesRepository(spark: SparkSession) {
  private val tableName = "local.metadata.processed_files"

  def getProcessedFiles(jobName: String): Seq[String] = {
    import spark.implicits._

    spark.table(tableName)
      .filter($"job_name" === jobName && $"status" === "SUCCESS")
      .select("file_path")
      .as[String]
      .collect()
      .toSeq
  }

  def saveSuccess(jobName: String, files: Seq[String]): Unit = {
    import spark.implicits._

    val df = files.toDF("file_path")
      .withColumn("job_name", lit(jobName))
      .withColumn("ingestion_time", current_timestamp())
      .withColumn("status", lit("SUCCESS"))

    df.writeTo(tableName).append()
  }

}
