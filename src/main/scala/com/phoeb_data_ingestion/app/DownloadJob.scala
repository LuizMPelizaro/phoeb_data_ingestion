package com.phoeb_data_ingestion.app

import com.phoeb_data_ingestion.ingestion.FileDownload

import scala.util.Try
import java.nio.file.Path

class DownloadJob(downloader: FileDownload) {

  def run(configs: List[DownloadConfig]): Try[List[Path]] = {

    val results =
      for {
        config <- configs
        url <- config.urlBuilder()
      } yield {
        val fileName = url.split("/").last
        val fullPath = s"${config.destinationFolder}/$fileName"
        downloader.download(url, fullPath)
      }

    sequence(results)
  }

  private def sequence[A](list: List[Try[A]]): Try[List[A]] =
    list.foldRight(Try(List.empty[A])) { (elem, acc) =>
      for {
        x <- elem
        xs <- acc
      } yield x :: xs
    }
}