package sorting

import common.Record
import common.KeyOrdering

object Partitioner {

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
