package common

case class Record(key: Array[Byte], value: Array[Byte])

object Record {
  val KeySize    = 10
  val ValueSize  = 90
  val RecordSize = KeySize + ValueSize

  def fromBytes(bytes: Array[Byte]): Record = {
    val key   = bytes.slice(0, KeySize)
    val value = bytes.slice(KeySize, RecordSize)
    Record(key, value)
  }

  def toBytes(r: Record): Array[Byte] = {
    val buf = new Array[Byte](RecordSize)
    System.arraycopy(r.key,   0, buf, 0,        KeySize)
    System.arraycopy(r.value, 0, buf, KeySize,  ValueSize)
    buf
  }

  def encodeSeq(rs: Seq[Record]): Array[Byte] = {
    val recSize = RecordSize
    val outBuf  = new Array[Byte](rs.size * recSize)
    var i       = 0
    rs.foreach { r =>
      val bytes = toBytes(r)
      System.arraycopy(bytes, 0, outBuf, i * recSize, recSize)
      i += 1
    }
    outBuf
  }

  def decodeBytes(bytes: Array[Byte]): Vector[Record] = {
    val recSize = RecordSize
    val count   = bytes.length / recSize
    val buf     = Vector.newBuilder[Record]

    var i = 0
    while (i < count) {
      val slice = java.util.Arrays.copyOfRange(bytes, i * recSize, (i + 1) * recSize)
      buf += Record.fromBytes(slice)
      i += 1
    }
    buf.result()
  }

  def writeFile(path: String, records: Seq[Record]): Unit = {
    val out = new java.io.FileOutputStream(path)
    try {
      records.foreach { r =>
        out.write(toBytes(r))
      }
    } finally {
      out.close()
    }
  }

}