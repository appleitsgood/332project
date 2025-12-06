package worker

import common.{PartitionPlan, Record}

final case class RemoteWorkerInfo(workerId: String, host: String, port: Int)

object WorkerState {
  @volatile private var sortedRecords: Option[Seq[Record]]             = None
  @volatile private var partitionPlan: Option[PartitionPlan]           = None
  @volatile private var receivedPartitions: Map[Int, Vector[Record]]   = Map.empty
  @volatile private var localWorkerId: Option[String]                = None
  @volatile private var workerEndpoints: Vector[RemoteWorkerInfo]      = Vector.empty
  @volatile private var outputDir: Option[String]                    = None
  @volatile private var masterEndpoint: Option[(String, Int)]        = None

  def setSortedRecords(recs: Seq[Record]): Unit = synchronized {
    sortedRecords = Some(recs)
  }

  def getSortedRecords: Option[Seq[Record]] = sortedRecords

  def setPartitionPlan(plan: PartitionPlan): Unit = synchronized {
    partitionPlan = Some(plan)
  }
  def getPartitionPlan: Option[PartitionPlan] = partitionPlan

  def addPartitionChunk(partIdx: Int, recs: Vector[Record]): Unit = synchronized {
    val prev = receivedPartitions.getOrElse(partIdx, Vector.empty)
    receivedPartitions = receivedPartitions.updated(partIdx, prev ++ recs)
  }

  def getPartition(partIdx: Int): Vector[Record] =
    receivedPartitions.getOrElse(partIdx, Vector.empty)

  def allPartitions: Map[Int, Vector[Record]] = receivedPartitions

  def setLocalWorkerId(id: String): Unit = synchronized { localWorkerId = Some(id) }

  def getLocalWorkerId: Option[String] = localWorkerId

  def setWorkers(ws: Vector[RemoteWorkerInfo]): Unit = synchronized { workerEndpoints = ws }

  def allWorkers: Vector[RemoteWorkerInfo] = workerEndpoints

  def numWorkers: Int = workerEndpoints.size

  def ownerOfPartition(idx: Int): Option[RemoteWorkerInfo] = workerEndpoints.lift(idx)

  def setMasterEndpoint(host: String, port: Int): Unit = synchronized {
    masterEndpoint = Some(host -> port)
  }
  def getMasterEndpoint: Option[(String, Int)] = masterEndpoint

  def setOutputDir(dir: String): Unit = synchronized {
    outputDir = Some(dir)
  }
  def getOutputDir: Option[String] = outputDir
}
