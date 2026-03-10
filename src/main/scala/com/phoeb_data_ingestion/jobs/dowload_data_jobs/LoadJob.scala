package com.phoeb_data_ingestion.jobs.dowload_data_jobs

import com.phoeb_data_ingestion.app._
import com.phoeb_data_ingestion.ingestion.FileDownload
import com.phoeb_data_ingestion.jobs.IngestionJob
import io.github.cdimascio.dotenv.Dotenv
import org.slf4j.LoggerFactory

import java.nio.file.Path
import java.time.LocalDate
import scala.util.Try

class LoadJob(downloader: FileDownload) extends IngestionJob {

  private val dotenv = Dotenv.configure().load()
  private val logger = LoggerFactory.getLogger(getClass)

  def run(): Try[List[Path]] = {

    logger.info("Starting LoadJob")

    val config = DownloadConfig(
      name = "load",
      destinationFolder = "data/raw/load",
      urlBuilder = () => {
        val link = Option(dotenv.get("LINK_CURVA_DE_CARGA"))
          .getOrElse{
            logger.error("LINK_CURVA_DE_CARGA not set")
            throw new IllegalArgumentException("LINK_CURVA_DE_CARGA not set")
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

        logger.info(s"Download range refined: $minYear to $maxYear")

        val urls = (minYear to maxYear).toList.flatMap{ year =>
          val url = s"$link$year.parquet"
          logger.debug(s"Generated annual URL: $url")
          List(url)
        }
        logger.debug(s"Total URLs generated: ${urls.size}")
        urls
      }
    )

    val result = new DownloadJob(downloader).run(List(config))

    result.fold(
      ex => logger.error("LoadJob failed", ex),
      paths => logger.info(s"LoadJob finished successfully. Files downloaded: ${paths.size}")
    )
    result
  }
}
