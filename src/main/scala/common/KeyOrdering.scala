package common

object KeyOrdering {
  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    var i = 0
    val n = math.min(a.length, b.length)
    while (i < n) {
      val x = a(i) & 0xff
      val y = b(i) & 0xff
      if (x != y) return x - y
      i += 1
    }
    a.length - b.length
  }

  implicit val recordOrdering: Ordering[Record] =
    (x: Record, y: Record) => compare(x.key, y.key)
}
