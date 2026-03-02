package com.phoeb_data_ingestion.app

import com.phoeb_data_ingestion.ingestion.FileDownload
import com.phoeb_data_ingestion.jobs._
import com.phoeb_data_ingestion.jobs.dowload_data_jobs.{GenerationJob, IngestionJob, LoadJob, SubSystemsENAJob}
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

    try {

      val downloader = new FileDownload(backend)

      // 🔥 Flag --all
      val jobNames: List[String] =
        if (args.contains("--all")) {
          logger.info("Flag --all detected. Running all jobs.")
          List("generation", "load", "subSystemEna")
        } else {
          args.toList
        }

      // Remove duplicados
      val distinctJobs = jobNames.distinct

      var hasFailure = false

      distinctJobs.foreach { jobName =>

        logger.info(s"Starting job: $jobName")

        val jobOpt: Option[IngestionJob] = jobName match {
          case "generation" => Some(new GenerationJob(downloader))
          case "load"       => Some(new LoadJob(downloader))
          case "subSystemEna" => Some(new SubSystemsENAJob(downloader))
          case other =>
            logger.error(s"Unknown job: $other")
            None
        }

        jobOpt.foreach { job =>
          job.run() match {
            case Success(paths) =>
              logger.info(
                s"Job '$jobName' finished successfully. Files downloaded: ${paths.size}"
              )

            case Failure(ex) =>
              hasFailure = true
              logger.error(
                s"Job '$jobName' failed with error: ${ex.getMessage}",
                ex
              )
          }
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
}