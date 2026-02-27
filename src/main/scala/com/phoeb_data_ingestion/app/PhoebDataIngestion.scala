package com.phoeb_data_ingestion.app

import com.phoeb_data_ingestion.ingestion.FileDownload
import com.phoeb_data_ingestion.jobs._

import sttp.client3.HttpURLConnectionBackend

import scala.util.{Success, Failure}

object PhoebDataIngestion {

  def main(args: Array[String]): Unit = {

    val jobName = args.headOption.getOrElse(
      throw new IllegalArgumentException("Job name not provided")
    )

    val backend = HttpURLConnectionBackend()

    try {

      val downloader = new FileDownload(backend)

      val job: IngestionJob = jobName match {
        case "generation" => new GenerationJob(downloader)
        case "load"       => new LoadJob(downloader)
        case _ => throw new IllegalArgumentException(s"Unknown job: $jobName")
      }

      job.run() match {
        case Success(paths) =>
          println(s"Job finalizado. Arquivos baixados: ${paths.size}")
        case Failure(ex) =>
          println(s"Erro no job: ${ex.getMessage}")
      }

    } finally {
      backend.close()
    }
  }
}