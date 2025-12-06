package common

import java.io.File

object InputFiles {
  def listInputFiles(path: String): Seq[File] = {
    val f = new File(path)
    if (!f.exists()) {
      sys.error(s"Input path not found: $path")
    }
    if (f.isDirectory) {
      f.listFiles()
        .filter(_.isFile)
        .sortBy(_.getName)
    } else {
      Seq(f)
    }
  }
}
