package worker

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString
import common.{PartitionPlan, Record}
import common.KeyOrdering._
import network.GrpcClients
import sorting.Partitioner
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, PartitionAck, PartitionChunk, PartitionChunkAck, StartMergeMsg, StartMergeAck, ShuffleDone, ShuffleDoneAck}

class WorkerService extends WorkerServiceGrpc.WorkerService {

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

    WorkerState.getSortedRecords.foreach { recs =>
      val buckets = Partitioner.partitionByPivots(recs, pivots)
      val sizes   = buckets.map(_.size)
      println(s"[WORKER] partitioned into ${buckets.size} buckets, sizes=$sizes")

      if (buckets.size != plan.numPartitions) {
        println(s"[WORKER] WARNING: buckets.size=${buckets.size} != plan.numPartitions=${plan.numPartitions}")
      }

      doShuffle(buckets)
    }

    notifyShuffleDoneToMaster()
    Future.successful(PartitionAck(ok = true))
  }

  private def doShuffle(buckets: Vector[Seq[Record]]): Unit = {
    val selfIdOpt = WorkerState.getLocalWorkerId
    val selfId    = selfIdOpt.getOrElse {
      println("[WORKER] WARNING: localWorkerId is not set; using 'unknown'")
      "unknown"
    }

    buckets.zipWithIndex.foreach { case (bucket, partIdx) =>
      if (bucket.nonEmpty) {
        WorkerState.ownerOfPartition(partIdx) match {
          case Some(owner) if owner.workerId == selfId =>
            WorkerState.addPartitionChunk(partIdx, bucket.toVector)
            println(s"[SHUFFLE] kept local partitionIdx=$partIdx, records=${bucket.size}")

          case Some(owner) =>
            sendPartitionChunk(owner.host, owner.port, selfId, partIdx, bucket)

          case None =>
            println(s"[SHUFFLE] WARNING: no owner for partitionIdx=$partIdx; skipping")
        }
      }
    }
  }

  private def sendPartitionChunk(host: String,
                                 port: Int,
                                 fromWorkerId: String,
                                 partIdx: Int,
                                 records: Seq[Record]): Unit = {

    val outBuf = Record.encodeSeq(records)

    val (ch, stub) = GrpcClients.workerClient(host, port)

    try {
      val req = PartitionChunk(
        fromWorkerId = fromWorkerId,
        partitionIdx = partIdx,
        data         = ByteString.copyFrom(outBuf)
      )

      val ack = Await.result(stub.receivePartitionChunk(req), 30.seconds)
      println(s"[SHUFFLE] sent partitionIdx=$partIdx, records=${records.size} " +
        s"to $host:$port (from=$fromWorkerId), ack.ok=${ack.ok}")

    } catch {
      case e: Throwable =>
        println(s"[SHUFFLE] ERROR: failed to send partitionIdx=$partIdx to $host:$port: ${e.getMessage}")
    } finally {
      ch.shutdownNow()
    }
  }

  override def receivePartitionChunk(req: PartitionChunk): Future[PartitionChunkAck] = {
    val recs: Vector[Record] = Record.decodeBytes(req.data.toByteArray)
    WorkerState.addPartitionChunk(req.partitionIdx, recs)

    println(s"[WORKER] received PartitionChunk from=${req.fromWorkerId}, " +
      s"partitionIdx=${req.partitionIdx}, records=${recs.size}")

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

    val outPath = Paths.get(outputDir)
    Files.createDirectories(outPath)

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

    ownedPartitions.sorted.foreach { partIdx =>
      val recs: Vector[Record] = WorkerState.getPartition(partIdx)
      val sorted = recs.sorted

      val filePath = outPath.resolve(s"partition.$partIdx").toString
      Record.writeFile(filePath, sorted)

      println(s"[MERGE] wrote ${sorted.size} records to $filePath (partitionIdx=$partIdx)")
    }
  }
}
