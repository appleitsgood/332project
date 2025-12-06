package worker

import common.{PartitionPlan, Record}

final case class RemoteWorkerInfo(workerId: String, host: String, port: Int)

object WorkerState {
  @volatile private var partitionPlan: Option[PartitionPlan]           = None
  @volatile private var receivedPartitions: Map[Int, Vector[Record]]   = Map.empty
  @volatile private var localWorkerId: Option[String]                  = None
  @volatile private var workerEndpoints: Vector[RemoteWorkerInfo]      = Vector.empty
  @volatile private var outputDir: Option[String]                      = None
  @volatile private var masterEndpoint: Option[(String, Int)]          = None
  @volatile private var inputFiles: Vector[String]                     = Vector.empty

  def setPartitionPlan(plan: PartitionPlan): Unit = synchronized {
    partitionPlan = Some(plan)
  }
  def getPartitionPlan: Option[PartitionPlan] = partitionPlan

  def addPartitionChunk(partitionIndex: Int, records: Vector[Record]): Unit = synchronized {
    val previous = receivedPartitions.getOrElse(partitionIndex, Vector.empty)
    receivedPartitions = receivedPartitions.updated(partitionIndex, previous ++ records)
  }

  def getPartition(partitionIndex: Int): Vector[Record] =
    receivedPartitions.getOrElse(partitionIndex, Vector.empty)

  def allPartitions: Map[Int, Vector[Record]] = receivedPartitions

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
}
