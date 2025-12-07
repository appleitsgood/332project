package master

import common.{PartitionPlan, Record}

final case class WorkerInfo(workerId: String, host: String, port: Int)

object MasterState {
  @volatile private var workers: Vector[WorkerInfo]            = Vector.empty
  @volatile private var expectedWorkers: Option[Int]           = None
  @volatile private var samples: Map[String, Vector[Record]]   = Map.empty
  @volatile private var partitionPlan: Option[PartitionPlan]   = None
  @volatile private var shuffleDone: Set[String]               = Set.empty
  @volatile private var startNanos: Option[Long]               = None
  @volatile private var allRegisteredNanos: Option[Long]       = None

  def init(expected: Option[Int]): Unit = synchronized {
    expectedWorkers = expected
    workers        = Vector.empty
    samples        = Map.empty
    partitionPlan  = None
    shuffleDone    = Set.empty
    startNanos     = None
    allRegisteredNanos = None
  }

  def addWorker(w: WorkerInfo): Int = synchronized {
    workers = workers :+ w
    workers.size
  }

  def allWorkers: Vector[WorkerInfo] = workers
  def expected: Option[Int]          = expectedWorkers

  def addSamples(workerId: String, recs: Vector[Record]): Unit = synchronized {
    val prev = samples.getOrElse(workerId, Vector.empty)
    samples = samples.updated(workerId, prev ++ recs)
  }

  def allSamplesByWorker: Map[String, Vector[Record]] = samples
  def samplingCompleteWorkers: Int                    = samples.size

  def setPartitionPlan(plan: PartitionPlan): Unit = synchronized {
    partitionPlan = Some(plan)
  }
  def getPartitionPlan: Option[PartitionPlan] = partitionPlan

  def markShuffleDone(workerId: String): Unit = synchronized {
    shuffleDone = shuffleDone + workerId
  }

  def shuffleDoneCount: Int = shuffleDone.size

  def allShuffleDone: Boolean =
    expectedWorkers.exists(total => shuffleDone.size == total)

  def setStartNanos(nanos: Long): Unit = synchronized { startNanos = Some(nanos) }
  def getStartNanos: Option[Long] = startNanos

  def markAllRegistered(nanos: Long): Unit = synchronized {
    allRegisteredNanos = Some(nanos)
  }
  def getAllRegisteredNanos: Option[Long] = allRegisteredNanos
}
