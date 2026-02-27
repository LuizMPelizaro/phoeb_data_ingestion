package com.phoeb_data_ingestion.ingestion

import scala.util.{Try, Success, Failure}
import sttp.client3._
import java.nio.file.{Files, Paths, Path}


class FileDownload(backend: SttpBackend[Identity, _]) {

  def download(link: String, filePath: String) : Try[Path] ={

    val request = basicRequest
      .get(uri"$link")
      .response(asByteArray)

    Try(request.send(backend))
      .flatMap {response =>
        response.body match {
          case Right(bytes) =>
            saveFile(bytes, filePath)

          case Left(error) =>
            Failure(new RuntimeException(s"Error HTTP: $error"))
        }
      }

  }

  private def saveFile(bytesArray: Array[Byte], filePath: String):  Try[Path] =
    Try {
      val path = Paths.get(filePath)

      if (path.getParent != null){
        // Se o dir nao existir cria
        Files.createDirectories(path.getParent)
      }
      Files.write(path,bytesArray)
      path
    }
}
