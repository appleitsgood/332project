package sorting

import common.{Record, Record => R, BlockReader, KeyOrdering}

object Sorter {
  private val BlockSize = 32 * 1024 * 1024
  private val RecSize   = Record.RecordSize

  def localSort(path: String): Seq[Record] = {
    val reader = new BlockReader(path, BlockSize)
    val acc    = scala.collection.mutable.ArrayBuffer.empty[Record]

    try {
      var done = false
      while (!done) {
        reader.readBlock() match {
          case Some(bytes) =>
            val count = bytes.length / RecSize
            var i = 0
            while (i < count) {
              val slice = bytes.slice(i * RecSize, (i + 1) * RecSize)
              acc += Record.fromBytes(slice)
              i += 1
            }
          case None =>
            done = true
        }
      }
      acc.toSeq.sorted(KeyOrdering.recordOrdering)
    } finally {
      reader.close()
    }
  }
}
