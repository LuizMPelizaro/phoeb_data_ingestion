package com.phoeb_data_ingestion.app


case class DownloadConfig(
                           name: String,
                           destinationFolder: String,
                           urlBuilder: () => List[String]
                         )
