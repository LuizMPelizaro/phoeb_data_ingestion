package com.phoeb_data_ingestion.app

import com.phoeb_data_ingestion.config.{SparkConfigLoader, SparkSessionFactory}
import com.phoeb_data_ingestion.ingestion.FileDownload
import com.phoeb_data_ingestion.jobs._
import com.phoeb_data_ingestion.jobs.bronze.{BronzeEnaSubSystemsJob, BronzeGenerationJob, BronzeLoadJob}
import com.phoeb_data_ingestion.jobs.dowload_data_jobs.{GenerationJob, LoadJob, SubSystemsENAJob}
import com.phoeb_data_ingestion.metadata.TableBootstrap
import io.github.cdimascio.dotenv.Dotenv
import org.slf4j.LoggerFactory
import sttp.client3.HttpURLConnectionBackend

import scala.util.{Failure, Success}

object PhoebDataIngestion {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      logger.error("No job provided. Use job names or --all")
      System.exit(1)
    }

    logger.info(s"Application started with args: ${args.mkString(", ")}")

    val backend = HttpURLConnectionBackend()

    val sparkConfig = SparkConfigLoader.load()
    val spark = SparkSessionFactory.create(sparkConfig)

    try {
      TableBootstrap.ensureMetadataTables(spark)
      val downloader = new FileDownload(backend)
      val dotenv = Dotenv.configure().ignoreIfMissing().load()

      val bronzeEnaSubSystemsJobPath = Option(dotenv.get("BRONZE_ENA_PATH"))
        .getOrElse("data/raw/subsystems")

      val bronzeEnaSubSystemsTableName = Option(dotenv.get("BRONZE_ENA_NAME"))
        .getOrElse("ena_subsystem")

      val bronzeGenerationJobPath = Option(dotenv.get("BRONZE_GENERATION_PATH"))
        .getOrElse("data/raw/generation")

      val bronzeGenerationTableName = Option(dotenv.get("BRONZE_GENERATION_NAME"))
        .getOrElse("generation")

      val bronzeLoadJobPath = Option(dotenv.get("BRONZE_LOAD_PATH"))
        .getOrElse("data/raw/load")

      val bronzeLoadJobTableName = Option(dotenv.get("BRONZE_LOAD_NAME"))
        .getOrElse("load")

      val runtimeArgs = args.toList

      val jobNames: List[String] =
        if (runtimeArgs.contains("--all")) {
          logger.info("Flag --all detected. Running all jobs.")
          List("generation", "load", "subSystemEna", "bronze-ena","bronze-generation","bronze-load")
        } else {
          runtimeArgs
        }

      val distinctJobs = jobNames.distinct

      var hasFailure = false

      distinctJobs.foreach { jobName =>

        logger.info(s"Starting job: $jobName")

        jobName match {
          case "generation" =>
            runDownloadJob(jobName, new GenerationJob(downloader)) match {
              case Failure(_) => hasFailure = true
              case Success(_) =>
            }

          case "load" =>
            runDownloadJob(jobName, new LoadJob(downloader)) match {
              case Failure(_) => hasFailure = true
              case Success(_) =>
            }

          case "subSystemEna" =>
            runDownloadJob(jobName, new SubSystemsENAJob(downloader)) match {
              case Failure(_) => hasFailure = true
              case Success(_) =>
            }

          case "bronze-ena" =>

            val bronzeEnaSubJobClass = new BronzeEnaSubSystemsJob(
              spark,
              bronzeEnaSubSystemsJobPath,
              bronzeEnaSubSystemsTableName
            )

            bronzeEnaSubJobClass.run() match {
              case Success(_) =>
                logger.info(s"Job '$jobName' finished successfully.")

              case Failure(ex) =>
                hasFailure = true
                logger.error(
                  s"Job '$jobName' failed with error: ${ex.getMessage}",
                  ex
                )
            }

          case "bronze-generation" =>

            val bronzeGenerationJobClass = new BronzeGenerationJob(
              spark,
              bronzeGenerationJobPath,
              bronzeGenerationTableName
            )

            bronzeGenerationJobClass.run() match {
              case Success(_) =>
                logger.info(s"Job '$jobName' finished successfully.")

              case Failure(ex) =>
                hasFailure = true
                logger.error(
                  s"Job '$jobName' failed with error: ${ex.getMessage}",
                  ex
                )
            }

          case "bronze-load" =>

            val bronzeLoadJobClass = new BronzeLoadJob(
              spark,
              bronzeLoadJobPath,
              bronzeLoadJobTableName
            )

            bronzeLoadJobClass.run() match {
              case Success(_) =>
                logger.info(s"Job '$jobName' finished successfully.")

              case Failure(ex) =>
                hasFailure = true
                logger.error(
                  s"Job '$jobName' failed with error: ${ex.getMessage}",
                  ex
                )
            }

          case other =>
            hasFailure = true
            logger.error(s"Unknown job: $other")
        }
      }

      logger.info("All requested jobs processed.")

      if (hasFailure) {
        logger.error("One or more jobs failed.")
        System.exit(1)
      } else {
        logger.info("Application finished successfully.")
        System.exit(0)
      }

    } catch {
      case ex: Exception =>
        logger.error("Unexpected fatal error", ex)
        System.exit(1)

    } finally {
      logger.info("Closing HTTP backend...")
      backend.close()
    }
  }

  private def runDownloadJob(jobName: String, job: IngestionJob): scala.util.Try[Unit] = {
    job.run() match {
      case Success(paths) =>
        logger.info(
          s"Job '$jobName' finished successfully. Files downloaded: ${paths.size}"
        )
        Success(())

      case Failure(ex) =>
        logger.error(
          s"Job '$jobName' failed with error: ${ex.getMessage}",
          ex
        )
        Failure(ex)
    }
  }
}
