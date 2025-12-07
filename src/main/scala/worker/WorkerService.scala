package worker

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

import com.google.protobuf.ByteString
import common.{PartitionPlan, Record}
import network.GrpcClients
import sorting.{Partitioner, Merger}
import common.RecordStream
import common.KeyOrdering._
import scala.collection.mutable.Builder
import org.slf4j.LoggerFactory
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, PartitionAck, PartitionChunk, PartitionChunkAck, StartMergeMsg, StartMergeAck, ShuffleDone, ShuffleDoneAck}

class WorkerService extends WorkerServiceGrpc.WorkerService {
  private val log = LoggerFactory.getLogger(getClass)
  private val ShuffleChunkBytes   = 3 * 1024 * 1024 + 512 * 1024 // 3.5MB
  private val RecordSize          = Record.RecordSize

  override def receivePartitionPlan(req: PartitionPlanMsg): Future[PartitionAck] = {
    val pivots: Vector[Record] =
      req.pivotKeys.map { bs =>
        val key      = bs.toByteArray
        val valBytes = new Array[Byte](Record.ValueSize)
        Record(key, valBytes)
      }.toVector

    val plan = PartitionPlan.fromPivots(pivots)
    WorkerState.setPartitionPlan(plan)

    if (req.endpoints.nonEmpty) {
      val workers: Vector[RemoteWorkerInfo] =
        req.endpoints.map { e =>
          RemoteWorkerInfo(
            workerId = e.workerId,
            host     = e.host,
            port     = e.port
          )
        }.toVector

      WorkerState.setWorkers(workers)
      log.info(s"[WORKER] received ${workers.size} worker endpoints for ${plan.numPartitions} partitions")
    } else {
      log.warn("[WORKER] warning: received PartitionPlan with no worker endpoints")
    }

    log.info(s"[WORKER] received PartitionPlan: pivots=${pivots.size}, numPartitions=${plan.numPartitions}")

    streamShuffle(inputFiles = WorkerState.getInputFiles, pivots = pivots, plan = plan)

    notifyShuffleDoneToMaster()
    Future.successful(PartitionAck(ok = true))
  }

  private def streamShuffle(inputFiles: Vector[String], pivots: Vector[Record], plan: PartitionPlan): Unit = {
    val selfIdOpt = WorkerState.getLocalWorkerId
    val selfId    = selfIdOpt.getOrElse {
      log.warn("[WORKER] WARNING: localWorkerId is not set; using 'unknown'")
      "unknown"
    }

    if (inputFiles.isEmpty) {
      log.warn("[WORKER] WARNING: no input files set; skipping shuffle")
      return
    }

    val numPartitions = plan.numPartitions
    if (WorkerState.allWorkers.isEmpty) {
      log.warn("[WORKER] WARNING: no worker endpoints; cannot shuffle")
      return
    }

    val shuffleStart = System.nanoTime()

    val buffers: Array[Builder[Record, Vector[Record]]] =
      Array.fill[Builder[Record, Vector[Record]]](numPartitions)(Vector.newBuilder[Record])
    val counts = Array.fill(numPartitions)(0)

    RecordStream.forEachRecord(inputFiles, ShuffleChunkBytes, warnOnPartial = true) { record =>
      val partitionIndex = Partitioner.partitionIndex(record, pivots)
      buffers(partitionIndex) += record
      counts(partitionIndex) += 1
      if (counts(partitionIndex) * RecordSize >= ShuffleChunkBytes) {
        flushPartition(partitionIndex, buffers, counts, selfId)
      }
    }

    var partitionIndex = 0
    while (partitionIndex < numPartitions) {
      if (counts(partitionIndex) > 0) {
        flushPartition(partitionIndex, buffers, counts, selfId)
      }
      partitionIndex += 1
    }

    val shuffleMillis = (System.nanoTime() - shuffleStart) / 1000000L
    WorkerState.setShuffleMillis(shuffleMillis)
    log.info(s"[SHUFFLE] completed in ${shuffleMillis} ms")
  }

  private def flushPartition(partitionIndex: Int,
                             buffers: Array[Builder[Record, Vector[Record]]],
                             counts: Array[Int],
                             selfId: String): Unit = {

    val chunk = buffers(partitionIndex).result()

    if (chunk.nonEmpty) {
      WorkerState.ownerOfPartition(partitionIndex) match {
        case Some(owner) if owner.workerId == selfId =>
          WorkerState.addPartitionChunk(partitionIndex, chunk)
          log.info(s"[SHUFFLE] kept local partitionIdx=$partitionIndex, records=${chunk.size}")

        case Some(owner) =>
          sendPartitionChunk(owner.host, owner.port, selfId, partitionIndex, chunk)

        case None =>
          log.warn(s"[SHUFFLE] WARNING: no owner for partitionIdx=$partitionIndex; dropping ${chunk.size} records")
      }
    }

    buffers(partitionIndex).clear()
    counts(partitionIndex) = 0
  }

  private def sendPartitionChunk(host: String,
                                 port: Int,
                                 fromWorkerId: String,
                                 partitionIndex: Int,
                                 records: Seq[Record]): Unit = {

    val outBuf = Record.encodeSeq(records)

    val (ch, stub) = GrpcClients.workerClient(host, port)

    try {
      val req = PartitionChunk(
        fromWorkerId = fromWorkerId,
        partitionIdx = partitionIndex,
        data         = ByteString.copyFrom(outBuf)
      )

      val ack = Await.result(stub.receivePartitionChunk(req), 30.seconds)
      log.info(s"[SHUFFLE] sent partitionIdx=$partitionIndex, records=${records.size} " +
        s"to $host:$port (from=$fromWorkerId), ack.ok=${ack.ok}")

    } catch {
      case e: Throwable =>
        log.error(s"[SHUFFLE] ERROR: failed to send partitionIdx=$partitionIndex to $host:$port: ${e.getMessage}")
    } finally {
      ch.shutdownNow()
    }
  }

  override def receivePartitionChunk(req: PartitionChunk): Future[PartitionChunkAck] = {
    val records: Vector[Record] = Record.decodeBytes(req.data.toByteArray)
    WorkerState.addPartitionChunk(req.partitionIdx, records)

    log.info(s"[WORKER] received PartitionChunk from=${req.fromWorkerId}, " +
      s"partitionIdx=${req.partitionIdx}, records=${records.size}")

    Future.successful(PartitionChunkAck(ok = true))
  }

  private def notifyShuffleDoneToMaster(): Unit = {
    val maybeEndpoint = WorkerState.getMasterEndpoint
    val maybeId       = WorkerState.getLocalWorkerId

    (maybeEndpoint, maybeId) match {
      case (Some((host, port)), Some(workerId)) =>
        val (ch, stub) = GrpcClients.masterClient(host, port)
        try {
          val req = ShuffleDone(workerId = workerId)
          val ack = Await.result(stub.notifyShuffleDone(req), 30.seconds)
          log.info(s"[BARRIER] notified master of shuffle done, ack.ok=${ack.ok}")
        } catch {
          case e: Throwable =>
            log.error(s"[BARRIER] failed to notify master of shuffle done: ${e.getMessage}")
        } finally {
          ch.shutdownNow()
        }
      case _ =>
        log.warn("[BARRIER] WARNING: master endpoint or local workerId not set; cannot notify shuffle done")
    }
  }

  override def startMerge(req: StartMergeMsg): Future[StartMergeAck] = {
    log.info("[MERGE] StartMerge received. Merging owned partitions and writing output files...")

    val fut: Future[StartMergeAck] = Future {
      val mergeStart = System.nanoTime()
      try {
        mergeAndWriteOutputs()
        val mergeMillis = (System.nanoTime() - mergeStart) / 1000000L
        WorkerState.setMergeMillis(mergeMillis)
        log.info(s"[MERGE] done in ${mergeMillis} ms.")
        logPhaseSummary()
        StartMergeAck(ok = true)
      } catch {
        case e: Throwable =>
          log.error(s"[MERGE] ERROR during merge: ${e.getMessage}")
          StartMergeAck(ok = false)
      }
    }

    fut.andThen { case _ =>
      log.info("[MERGE] worker shutting down.")
      Thread.sleep(100)
      System.exit(0)
    }

    fut
  }

  private def mergeAndWriteOutputs(): Unit = {
    val outDirOpt = WorkerState.getOutputDir
    val selfIdOpt = WorkerState.getLocalWorkerId
    val planOpt = WorkerState.getPartitionPlan

    if (outDirOpt.isEmpty) {
      log.warn("[MERGE] WARNING: outputDir is not set; skip merge.")
      return
    }
    if (selfIdOpt.isEmpty) {
      log.warn("[MERGE] WARNING: localWorkerId is not set; skip merge.")
      return
    }
    if (planOpt.isEmpty) {
      log.warn("[MERGE] WARNING: partitionPlan is not set; skip merge.")
      return
    }

    val outputDir = outDirOpt.get
    val selfId = selfIdOpt.get
    val plan = planOpt.get

    val ownedPartitions: Seq[Int] =
      (0 until plan.numPartitions).flatMap { idx =>
        WorkerState.ownerOfPartition(idx) match {
          case Some(owner) if owner.workerId == selfId => Some(idx)
          case _ => None
        }
      }

    if (ownedPartitions.isEmpty) {
      log.warn(s"[MERGE] WARNING: no owned partitions for workerId=$selfId")
      return
    }

    Merger.writePartitions(outputDir, ownedPartitions.sorted, idx => WorkerState.getPartition(idx))
  }

  private def logPhaseSummary(): Unit = {
    val sampleMs  = WorkerState.getSampleMillis.getOrElse(-1L)
    val shuffleMs = WorkerState.getShuffleMillis.getOrElse(-1L)
    val mergeMs   = WorkerState.getMergeMillis.getOrElse(-1L)

    val sampleStr  = if (sampleMs >= 0) s"${sampleMs} ms" else "n/a"
    val shuffleStr = if (shuffleMs >= 0) s"${shuffleMs} ms" else "n/a"
    val mergeStr   = if (mergeMs >= 0) s"${mergeMs} ms" else "n/a"

    log.info(s"[PHASE] sample=$sampleStr, shuffle=$shuffleStr, merge=$mergeStr")
  }
}
