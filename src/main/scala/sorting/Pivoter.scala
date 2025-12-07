package sorting

import common.Record
import common.KeyOrdering

object Pivoter {

  def choosePivots(samplesByWorker: Seq[Seq[Record]], numWorkers: Int): Vector[Record] = {
    if (numWorkers <= 1) return Vector.empty

    val allSamples = samplesByWorker.flatten.sorted(KeyOrdering.recordOrdering)
    if (allSamples.isEmpty) return Vector.empty

    val total       = allSamples.length
    val pivotCount  = numWorkers - 1
    val pivotsBuf   = Vector.newBuilder[Record]

    for (i <- 1 to pivotCount) {
      val pos = ((i.toDouble * total) / numWorkers).toInt.min(total - 1)
      pivotsBuf += allSamples(pos)
    }

    pivotsBuf.result()
  }
}
