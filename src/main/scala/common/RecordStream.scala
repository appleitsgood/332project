package common

object RecordStream {

  def forEachRecord(paths: Seq[String], blockSize: Int, warnOnPartial: Boolean)
                   (handler: Record => Unit): Unit = {
    val recordSize = Record.RecordSize

    var pathIdx = 0
    while (pathIdx < paths.length) {
      val path    = paths(pathIdx)
      val reader  = new BlockReader(path, blockSize)
      var leftover = Array.emptyByteArray

      try {
        var maybeBlock = reader.readBlock()
        while (maybeBlock.isDefined) {
          val block    = maybeBlock.get
          val combined = new Array[Byte](leftover.length + block.length)

          if (leftover.nonEmpty) {
            System.arraycopy(leftover, 0, combined, 0, leftover.length)
          }
          System.arraycopy(block, 0, combined, leftover.length, block.length)

          val count = combined.length / recordSize

          var i = 0
          while (i < count) {
            val offset = i * recordSize
            val slice  = java.util.Arrays.copyOfRange(combined, offset, offset + recordSize)
            val record = Record.fromBytes(slice)
            handler(record)
            i += 1
          }

          val remainder = combined.length % recordSize
          if (remainder > 0) {
            leftover = java.util.Arrays.copyOfRange(combined, combined.length - remainder, combined.length)
          } else {
            leftover = Array.emptyByteArray
          }

          maybeBlock = reader.readBlock()
        }
      } finally {
        reader.close()
      }

      if (leftover.nonEmpty && warnOnPartial) {
        println(s"[RecordStream] WARNING: leftover ${leftover.length} bytes in file $path (ignored)")
      }

      pathIdx += 1
    }
  }
}
