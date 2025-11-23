package common

import java.io.{FileInputStream, BufferedInputStream}

class BlockReader(path: String, blockSize: Int) {
  private val in  = new BufferedInputStream(new FileInputStream(path))
  private val buf = new Array[Byte](blockSize)

  def readBlock(): Option[Array[Byte]] = {
    val n = in.read(buf)
    if (n == -1) None
    else Some(buf.slice(0, n))
  }

  def close(): Unit = in.close()
}
