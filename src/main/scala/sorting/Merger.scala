package sorting

import java.nio.file.{Files, Paths}
import common.{Record}
import common.KeyOrdering._

object Merger {

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

      println(s"[MERGE] wrote ${sorted.size} records to $filePath (partitionIdx=$partitionIndex)")
      idx += 1
    }
  }
}
