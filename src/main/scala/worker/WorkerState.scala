package worker

import common.PartitionPlan

final case class RemoteWorkerInfo(workerId: String, host: String, port: Int)

object WorkerState {
  @volatile private var partitionPlan: Option[PartitionPlan]           = None
  @volatile private var localWorkerId: Option[String]                  = None
  @volatile private var workerEndpoints: Vector[RemoteWorkerInfo]      = Vector.empty
  @volatile private var outputDir: Option[String]                      = None
  @volatile private var masterEndpoint: Option[(String, Int)]          = None
  @volatile private var inputFiles: Vector[String]                     = Vector.empty
  @volatile private var sampleMillis: Option[Long]                     = None
  @volatile private var shuffleMillis: Option[Long]                    = None
  @volatile private var mergeMillis: Option[Long]                      = None
  @volatile private var tempDir: Option[java.io.File]                  = None
  @volatile private var partitionTemp: Map[Int, java.io.File]          = Map.empty
  @volatile private var partitionCounts: Map[Int, Long]                = Map.empty

  def setPartitionPlan(plan: PartitionPlan): Unit = synchronized {
    partitionPlan = Some(plan)
  }
  def getPartitionPlan: Option[PartitionPlan] = partitionPlan

  def setLocalWorkerId(id: String): Unit = synchronized { localWorkerId = Some(id) }

  def getLocalWorkerId: Option[String] = localWorkerId

  def setWorkers(ws: Vector[RemoteWorkerInfo]): Unit = synchronized { workerEndpoints = ws }

  def allWorkers: Vector[RemoteWorkerInfo] = workerEndpoints

  def numWorkers: Int = workerEndpoints.size

  def ownerOfPartition(partitionIndex: Int): Option[RemoteWorkerInfo] = workerEndpoints.lift(partitionIndex)

  def setMasterEndpoint(host: String, port: Int): Unit = synchronized {
    masterEndpoint = Some(host -> port)
  }
  def getMasterEndpoint: Option[(String, Int)] = masterEndpoint

  def setOutputDir(dir: String): Unit = synchronized {
    outputDir = Some(dir)
  }
  def getOutputDir: Option[String] = outputDir

  def setInputFiles(paths: Vector[String]): Unit = synchronized {
    inputFiles = paths
  }
  def getInputFiles: Vector[String] = inputFiles

  def setSampleMillis(ms: Long): Unit = synchronized { sampleMillis = Some(ms) }
  def getSampleMillis: Option[Long] = sampleMillis

  def setShuffleMillis(ms: Long): Unit = synchronized { shuffleMillis = Some(ms) }
  def getShuffleMillis: Option[Long] = shuffleMillis

  def setMergeMillis(ms: Long): Unit = synchronized { mergeMillis = Some(ms) }
  def getMergeMillis: Option[Long] = mergeMillis

  def setTempDir(dir: java.io.File): Unit = synchronized {
    tempDir = Some(dir)
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }
  def getTempDir: Option[java.io.File] = tempDir

  def appendPartitionBytes(partitionIndex: Int, data: Array[Byte]): Unit = synchronized {
    val dir = tempDir.getOrElse {
      throw new IllegalStateException("tempDir not set")
    }
    val file = partitionTemp.getOrElse(partitionIndex, {
      val f = new java.io.File(dir, s"partition.$partitionIndex.tmp")
      partitionTemp = partitionTemp.updated(partitionIndex, f)
      f
    })

    val out = new java.io.FileOutputStream(file, true)
    try {
      out.write(data)
    } finally {
      out.close()
    }

    val prevCount = partitionCounts.getOrElse(partitionIndex, 0L)
    partitionCounts = partitionCounts.updated(partitionIndex, prevCount + data.length)
  }

  def getPartitionFile(partitionIndex: Int): Option[java.io.File] =
    partitionTemp.get(partitionIndex)

  def clearTemp(): Unit = synchronized {
    partitionTemp.values.foreach { f =>
      if (f.exists()) f.delete()
    }
    tempDir.foreach { dir =>
      val files = dir.listFiles()
      if (files != null) {
        var i = 0
        while (i < files.length) {
          files(i).delete()
          i += 1
        }
      }
      dir.delete()
    }
    partitionTemp = Map.empty
    partitionCounts = Map.empty
    tempDir = None
  }
}
