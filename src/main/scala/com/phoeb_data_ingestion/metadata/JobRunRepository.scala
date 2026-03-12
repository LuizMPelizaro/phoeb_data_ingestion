package com.phoeb_data_ingestion.metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class JobRunRepository(spark: SparkSession) {

  private val tableName = "local.metadata.job_runs"

  def startRun(jobName: String): Unit = {
    import spark.implicits._

    Seq(jobName)
      .toDF("job_name")
      .withColumn("start_time", current_timestamp())
      .withColumn("status", lit("RUNNING"))
      .withColumn("end_time", lit(null).cast("timestamp"))
      .writeTo(tableName)
      .append()
  }

  def finishRun(jobName: String, status: String): Unit = {
    spark.sql(
      s"""
         |MERGE INTO $tableName t
         |USING (SELECT '$jobName' as job_name) s
         |ON t.job_name = s.job_name AND t.status = 'RUNNING'
         |WHEN MATCHED THEN UPDATE SET
         |  t.status = '$status',
         |  t.end_time = current_timestamp()
       """.stripMargin
    )
  }
}