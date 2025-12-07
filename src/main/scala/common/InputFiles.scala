package common

import java.io.File

object InputFiles {
  def listInputFiles(path: String): Seq[File] = {
    val f = new File(path)
    if (!f.exists()) {
      sys.error(s"Input path not found: $path")
    }
    if (f.isDirectory) {
      val files = f.listFiles()
      if (files == null) {
        Seq.empty
      } else {
        val onlyFiles = files.iterator.filter(_.isFile).toArray
        val sorted    = onlyFiles.sortBy(_.getName)
        sorted.toSeq
      }
    } else {
      Seq(f)
    }
  }
}
