package sorting

import common.Record
import common.KeyOrdering

object Partitioner {

  def partitionIndex(record: Record, pivots: Vector[Record]): Int = {
    var lo = 0
    var hi = pivots.length

    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val cmp = KeyOrdering.compare(record.key, pivots(mid).key)
      if (cmp < 0) {
        hi = mid
      } else {
        lo = mid + 1
      }
    }

    lo
  }

  def partitionByPivots(records: Seq[Record], pivots: Vector[Record]): Vector[Vector[Record]] = {
    implicit val ord = KeyOrdering.recordOrdering

    val numPartitions = pivots.length + 1
    val buckets = Array.fill(numPartitions)(Vector.newBuilder[Record])

    records.foreach { r =>
      val idx = pivots.indexWhere(p => ord.gt(p, r)) match {
        case -1 => numPartitions - 1
        case i  => i
      }
      buckets(idx) += r
    }

    buckets.map(_.result()).toVector
  }
}
