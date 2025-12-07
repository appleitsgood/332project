package sorting

import java.nio.file.{Files, Paths}
import common.{Record, KeyOrdering}
import common.KeyOrdering._
import org.slf4j.LoggerFactory
import java.io.{File, FileInputStream, FileOutputStream, BufferedInputStream}

object Merger {
  private val log = LoggerFactory.getLogger(getClass)

  def writePartitions(outputDir: String,
                      ownedPartitions: Seq[Int],
                      fetchPartition: Int => Unit): Unit = {

    val outPath = Paths.get(outputDir)
    Files.createDirectories(outPath)

    var idx = 0
    while (idx < ownedPartitions.length) {
      val partitionIndex = ownedPartitions(idx)
      fetchPartition(partitionIndex)
      val filePath = outPath.resolve(s"partition.$partitionIndex").toString
      log.info(s"[MERGE] wrote partitionIdx=$partitionIndex to $filePath")
      idx += 1
    }
  }

  def mergeSpilledPartition(tempDir: File,
                            spillFile: File,
                            outputDir: String,
                            partitionIndex: Int,
                            chunkLimit: Int,
                            blockSize: Int): Unit = {
    if (!spillFile.exists()) return

    val records  = scala.collection.mutable.ArrayBuffer.empty[Record]
    val runFiles = scala.collection.mutable.ArrayBuffer.empty[File]
    val reader   = new common.BlockReader(spillFile.getAbsolutePath, blockSize)
    val recordSize = Record.RecordSize
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
          records += Record.fromBytes(slice)
          if (records.length >= chunkLimit) {
            runFiles += writeSortedRun(tempDir, partitionIndex, runFiles.length, records)
            records.clear()
          }
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

      if (records.nonEmpty) {
        runFiles += writeSortedRun(tempDir, partitionIndex, runFiles.length, records)
        records.clear()
      }
      if (leftover.nonEmpty) {
        log.warn(s"[MERGE] WARNING: leftover ${leftover.length} bytes in spill ${spillFile.getName} (ignored)")
      }
    } finally {
      reader.close()
    }

    val outFile = new File(outputDir, s"partition.$partitionIndex")
    mergeRuns(runFiles.toVector, outFile)

    var i = 0
    while (i < runFiles.length) {
      val f = runFiles(i)
      if (f.exists()) f.delete()
      i += 1
    }
  }

  private def writeSortedRun(tmpDir: File,
                             partitionIndex: Int,
                             runIdx: Int,
                             records: scala.collection.mutable.ArrayBuffer[Record]): File = {
    val runFile = new File(tmpDir, s"partition.$partitionIndex.run.$runIdx")
    val sorted = records.sorted
    val out = new FileOutputStream(runFile)
    try {
      var i = 0
      while (i < sorted.length) {
        out.write(Record.toBytes(sorted(i)))
        i += 1
      }
    } finally {
      out.close()
    }
    runFile
  }

  private def mergeRuns(runs: Vector[File], outFile: File): Unit = {
    if (runs.isEmpty) {
      return
    }
    if (runs.length == 1) {
      val in  = new BufferedInputStream(new FileInputStream(runs.head))
      val out = new FileOutputStream(outFile)
      try {
        val buf = new Array[Byte](8 * 1024)
        var n = in.read(buf)
        while (n != -1) {
          out.write(buf, 0, n)
          n = in.read(buf)
        }
      } finally {
        in.close()
        out.close()
      }
      return
    }

    case class Cursor(in: BufferedInputStream, var current: Option[Record])

    val cursors = new Array[Cursor](runs.length)
    var idx = 0
    while (idx < runs.length) {
      val in = new BufferedInputStream(new FileInputStream(runs(idx)))
      val recOpt = readNextRecord(in)
      cursors(idx) = Cursor(in, recOpt)
      idx += 1
    }

    implicit val ord: Ordering[(Int, Record)] =
      Ordering.fromLessThan { (a, b) =>
        KeyOrdering.compare(a._2.key, b._2.key) < 0
      }

    val pq = scala.collection.mutable.PriorityQueue.empty[(Int, Record)](ord.reverse)

    idx = 0
    while (idx < cursors.length) {
      cursors(idx).current.foreach { r =>
        pq.enqueue((idx, r))
      }
      idx += 1
    }

    val out = new FileOutputStream(outFile)
    try {
      while (pq.nonEmpty) {
        val (cursorIdx, rec) = pq.dequeue()
        out.write(Record.toBytes(rec))

        val cursor = cursors(cursorIdx)
        cursor.current = readNextRecord(cursor.in)
        cursor.current.foreach { nextRec =>
          pq.enqueue((cursorIdx, nextRec))
        }
      }
    } finally {
      out.close()
      idx = 0
      while (idx < cursors.length) {
        cursors(idx).in.close()
        idx += 1
      }
    }
  }

  private def readNextRecord(in: BufferedInputStream): Option[Record] = {
    val buf = new Array[Byte](Record.RecordSize)
    val read = in.read(buf)
    if (read == Record.RecordSize) Some(Record.fromBytes(buf)) else None
  }
}
