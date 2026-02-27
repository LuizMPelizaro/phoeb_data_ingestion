package com.phoeb_data_ingestion.jobs

import org.slf4j.LoggerFactory
import com.phoeb_data_ingestion.app._
import com.phoeb_data_ingestion.ingestion.FileDownload
import io.github.cdimascio.dotenv.Dotenv

import scala.util.Try
import java.nio.file.Path

class GenerationJob(downloader: FileDownload) extends IngestionJob {

  private val dotenv = Dotenv.configure().ignoreIfMissing().load()
  private val logger = LoggerFactory.getLogger(getClass)

  def run(): Try[List[Path]] = {

    logger.info("Starting GenerationJob")

    val config = DownloadConfig(
      name = "generation",
      destinationFolder = "data/raw/generation",
      urlBuilder = () => {

        val link = Option(dotenv.get("LINK_GERACAO_USINA"))
          .getOrElse {
            logger.error("LINK_GERACAO_USINA not set")
            throw new IllegalArgumentException("LINK_GERACAO_USINA not set")
          }

        val minYear = dotenv.get("MIN_YEAR", "2020").toInt
        val maxYear = dotenv.get("MAX_YEAR", "2024").toInt

        logger.info(s"Download range defined: $minYear to $maxYear")

        if (minYear > maxYear) {
          logger.error("MIN_YEAR cannot be greater than MAX_YEAR")
          throw new IllegalArgumentException("MIN_YEAR cannot be greater than MAX_YEAR")
        }

        val urls = (minYear to maxYear).toList.flatMap { year =>
          if (year <= 2021) {
            val url = s"$link$year.parquet"
            logger.debug(s"Generated annual URL: $url")
            List(url)
          } else {
            (1 to 12).map { month =>
              val monthFormatted = f"$month%02d"
              val url = s"$link${year}_${monthFormatted}.parquet"
              logger.debug(s"Generated monthly URL: $url")
              url
            }
          }
        }

        logger.info(s"Total URLs generated: ${urls.size}")
        urls
      }
    )

    val result = new DownloadJob(downloader).run(List(config))

    result.fold(
      ex => logger.error("GenerationJob failed", ex),
      paths => logger.info(s"GenerationJob finished successfully. Files downloaded: ${paths.size}")
    )

    result
  }
}