package sorting

import common.Record
import common.KeyOrdering

object Sampler {

  def takeUniformSamples(records: Seq[Record], targetSamples: Int): Vector[Record] = {
    if (records.isEmpty || targetSamples <= 0) return Vector.empty

    val n = records.length
    val m = math.min(targetSamples, n)
    val stride = math.max(1, n / m)

    val buf = Vector.newBuilder[Record]
    var i   = 0
    while (i < n && buf.result().length < m) {
      buf += records(i)
      i += stride
    }
    buf.result()
  }
}
