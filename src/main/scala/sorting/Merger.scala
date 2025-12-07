package sorting

import java.nio.file.{Files, Paths}
import common.{Record}
import common.KeyOrdering._
import org.slf4j.LoggerFactory

object Merger {
  private val log = LoggerFactory.getLogger(getClass)

  def writePartitions(outputDir: String,
                      ownedPartitions: Seq[Int],
                      fetchPartition: Int => Vector[Record]): Unit = {

    val outPath = Paths.get(outputDir)
    Files.createDirectories(outPath)

    var idx = 0
    while (idx < ownedPartitions.length) {
      val partitionIndex = ownedPartitions(idx)
      val records: Vector[Record] = fetchPartition(partitionIndex)
      val sorted = records.sorted

      val filePath = outPath.resolve(s"partition.$partitionIndex").toString
      Record.writeFile(filePath, sorted)

      log.info(s"[MERGE] wrote ${sorted.size} records to $filePath (partitionIdx=$partitionIndex)")
      idx += 1
    }
  }
}
