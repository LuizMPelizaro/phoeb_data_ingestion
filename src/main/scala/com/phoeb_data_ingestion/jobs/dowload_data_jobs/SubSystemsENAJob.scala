package com.phoeb_data_ingestion.jobs.dowload_data_jobs

import com.phoeb_data_ingestion.app.{DownloadConfig, DownloadJob}
import com.phoeb_data_ingestion.ingestion.FileDownload
import io.github.cdimascio.dotenv.Dotenv
import org.slf4j.LoggerFactory

import java.time.LocalDate
import scala.util.Try
import java.nio.file.Path

class SubSystemsENAJob(downloader: FileDownload) extends  IngestionJob {

  private val dotenv = Dotenv.configure().load()
  private val logger = LoggerFactory.getLogger(getClass)


  def run(): Try[List[Path]] = {

    logger.info("Starting SubSystemsENAJob")

    val config = DownloadConfig(
      name = "SubSystemsENA",
      destinationFolder = "data/raw/subsystems",
      urlBuilder = () => {

        val link = Option(dotenv.get("LINK_ENA_SUBSISTEMA"))
          .getOrElse{
            logger.error("LINK_ENA_SUBSISTEMA not set")
            throw new IllegalArgumentException("LINK_ENA_SUBSISTEMA not set")
          }

        val minYear = dotenv.get("MIN_YEAR").toInt
        val maxYearEnv = dotenv.get("MAX_YEAR").toInt

        val today = LocalDate.now()
        val currentYear = today.getYear

        val maxYear =
          if (maxYearEnv > currentYear){
            logger.warn(
              s"MAX_YEAR ($maxYearEnv) is greater than current year ($currentYear). Adjusting."
            )
            currentYear
          }else maxYearEnv

        logger.info(s"Dowload range defined: $minYear to $maxYear")

        if (minYear > maxYear){
          logger.error("MIN_YEAR cannot be greater than MAX_YEAR")
          throw  new IllegalArgumentException("MIN_YEAR cannot be greater then MAX_YEAR")
        }

        val urls = (minYear to maxYear).toList.flatMap{year =>
          val url = s"$link$year.csv"
          logger.debug(s"Generated anual URL: $url")
          List(url)
        }
        logger.debug(s"Total URLs generated: ${urls.size}")
        urls
      }
    )

    val result = new DownloadJob(downloader).run(List(config))

    result.fold(
      ex => logger.error("SubSystemsENAJob failed", ex),
      paths => logger.info(s"LoadJob finished successfully. Files downloaded: ${paths.size}")
    )
    result
  }
}
