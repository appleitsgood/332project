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
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, PartitionAck, PartitionChunk, PartitionChunkAck, StartMergeMsg, StartMergeAck, ShuffleDone, ShuffleDoneAck}

class WorkerService extends WorkerServiceGrpc.WorkerService {
  private val ShuffleChunkBytes   = 4 * 1024 * 1024
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
      println(s"[WORKER] received ${workers.size} worker endpoints for ${plan.numPartitions} partitions")
    } else {
      println("[WORKER] warning: received PartitionPlan with no worker endpoints")
    }

    println(s"[WORKER] received PartitionPlan: pivots=${pivots.size}, numPartitions=${plan.numPartitions}")

    streamShuffle(inputFiles = WorkerState.getInputFiles, pivots = pivots, plan = plan)

    notifyShuffleDoneToMaster()
    Future.successful(PartitionAck(ok = true))
  }

  private def streamShuffle(inputFiles: Vector[String], pivots: Vector[Record], plan: PartitionPlan): Unit = {
    val selfIdOpt = WorkerState.getLocalWorkerId
    val selfId    = selfIdOpt.getOrElse {
      println("[WORKER] WARNING: localWorkerId is not set; using 'unknown'")
      "unknown"
    }

    if (inputFiles.isEmpty) {
      println("[WORKER] WARNING: no input files set; skipping shuffle")
      return
    }

    val numPartitions = plan.numPartitions
    if (WorkerState.allWorkers.isEmpty) {
      println("[WORKER] WARNING: no worker endpoints; cannot shuffle")
      return
    }

    val buffers: Array[scala.collection.mutable.Builder[Record, Vector[Record]]] =
      new Array[scala.collection.mutable.Builder[Record, Vector[Record]]](numPartitions)
    val counts = Array.fill(numPartitions)(0)

    var bufferIndex = 0
    while (bufferIndex < numPartitions) {
      buffers(bufferIndex) = Vector.newBuilder[Record]
      bufferIndex += 1
    }

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
  }

  private def flushPartition(partitionIndex: Int,
                             buffers: Array[scala.collection.mutable.Builder[Record, Vector[Record]]],
                             counts: Array[Int],
                             selfId: String): Unit = {

    val chunk = buffers(partitionIndex).result()

    if (chunk.nonEmpty) {
      WorkerState.ownerOfPartition(partitionIndex) match {
        case Some(owner) if owner.workerId == selfId =>
          WorkerState.addPartitionChunk(partitionIndex, chunk)
          println(s"[SHUFFLE] kept local partitionIdx=$partitionIndex, records=${chunk.size}")

        case Some(owner) =>
          sendPartitionChunk(owner.host, owner.port, selfId, partitionIndex, chunk)

        case None =>
          println(s"[SHUFFLE] WARNING: no owner for partitionIdx=$partitionIndex; dropping ${chunk.size} records")
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
      println(s"[SHUFFLE] sent partitionIdx=$partitionIndex, records=${records.size} " +
        s"to $host:$port (from=$fromWorkerId), ack.ok=${ack.ok}")

    } catch {
      case e: Throwable =>
        println(s"[SHUFFLE] ERROR: failed to send partitionIdx=$partitionIndex to $host:$port: ${e.getMessage}")
    } finally {
      ch.shutdownNow()
    }
  }

  override def receivePartitionChunk(req: PartitionChunk): Future[PartitionChunkAck] = {
    val records: Vector[Record] = Record.decodeBytes(req.data.toByteArray)
    WorkerState.addPartitionChunk(req.partitionIdx, records)

    println(s"[WORKER] received PartitionChunk from=${req.fromWorkerId}, " +
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
          println(s"[BARRIER] notified master of shuffle done, ack.ok=${ack.ok}")
        } catch {
          case e: Throwable =>
            println(s"[BARRIER] failed to notify master of shuffle done: ${e.getMessage}")
        } finally {
          ch.shutdownNow()
        }
      case _ =>
        println("[BARRIER] WARNING: master endpoint or local workerId not set; cannot notify shuffle done")
    }
  }

  override def startMerge(req: StartMergeMsg): Future[StartMergeAck] = {
    println("[MERGE] StartMerge received. Merging owned partitions and writing output files...")

    val fut: Future[StartMergeAck] = Future {
      try {
        mergeAndWriteOutputs()
        println("[MERGE] done.")
        StartMergeAck(ok = true)
      } catch {
        case e: Throwable =>
          println(s"[MERGE] ERROR during merge: ${e.getMessage}")
          StartMergeAck(ok = false)
      }
    }

    fut.andThen { case _ =>
      println("[MERGE] worker shutting down.")
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
      println("[MERGE] WARNING: outputDir is not set; skip merge.")
      return
    }
    if (selfIdOpt.isEmpty) {
      println("[MERGE] WARNING: localWorkerId is not set; skip merge.")
      return
    }
    if (planOpt.isEmpty) {
      println("[MERGE] WARNING: partitionPlan is not set; skip merge.")
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
      println(s"[MERGE] WARNING: no owned partitions for workerId=$selfId")
      return
    }

    Merger.writePartitions(outputDir, ownedPartitions.sorted, idx => WorkerState.getPartition(idx))
  }
}
