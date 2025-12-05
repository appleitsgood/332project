package worker

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._

import com.google.protobuf.ByteString
import common.{PartitionPlan, Record}
import network.GrpcClients
import sorting.Partitioner
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, PartitionAck, PartitionChunk, PartitionChunkAck}

class WorkerService extends WorkerServiceGrpc.WorkerService {

  override def receivePartitionPlan(req: PartitionPlanMsg): Future[PartitionAck] = {
    // 1) pivots 복원
    val pivots: Vector[Record] =
      req.pivotKeys.map { bs =>
        val key      = bs.toByteArray
        val valBytes = new Array[Byte](Record.ValueSize)
        Record(key, valBytes)
      }.toVector

    val plan = PartitionPlan.fromPivots(pivots)
    WorkerState.setPartitionPlan(plan)

    // 2) endpoints → RemoteWorkerInfo 로 변환해서 저장
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

      // numPartitions와 buckets 크기 불일치 시 경고
      if (buckets.size != plan.numPartitions) {
        println(s"[WORKER] WARNING: buckets.size=${buckets.size} != plan.numPartitions=${plan.numPartitions}")
      }

      // 실제 shuffle
      doShuffle(buckets)
    }

    Future.successful(PartitionAck(ok = true))
  }

  private def doShuffle(buckets: Vector[Seq[Record]]): Unit = {
    val selfIdOpt = WorkerState.getLocalWorkerId
    val selfId    = selfIdOpt.getOrElse {
      println("[WORKER] WARNING: localWorkerId is not set; using 'unknown'")
      "unknown"
    }

    buckets.zipWithIndex.foreach { case (bucket, partIdx) =>
      // 빈 버킷은 skip
      if (bucket.nonEmpty) {
        WorkerState.ownerOfPartition(partIdx) match {
          case Some(owner) if owner.workerId == selfId =>
            // 1) 내가 담당인 파티션이면, 바로 로컬에 쌓는다.
            WorkerState.addPartitionChunk(partIdx, bucket.toVector)
            println(s"[SHUFFLE] kept local partitionIdx=$partIdx, records=${bucket.size}")

          case Some(owner) =>
            // 2) 다른 워커에게 보내야 하는 파티션이라면 RPC 호출
            sendPartitionChunk(owner.host, owner.port, selfId, partIdx, bucket)

          case None =>
            // 매핑 정보가 없으면 경고만
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

    val recSize = Record.RecordSize
    val outBuf  = new Array[Byte](records.size * recSize)
    var i       = 0
    records.foreach { r =>
      val bytes = Record.toBytes(r)
      System.arraycopy(bytes, 0, outBuf, i * recSize, recSize)
      i += 1
    }

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
    val bytes   = req.data.toByteArray
    val recSize = Record.RecordSize
    val count   = bytes.length / recSize

    val buf = Vector.newBuilder[Record]
    var i   = 0
    while (i < count) {
      val slice = java.util.Arrays.copyOfRange(bytes, i * recSize, (i + 1) * recSize)
      buf += Record.fromBytes(slice)
      i += 1
    }
    val recs = buf.result()

    WorkerState.addPartitionChunk(req.partitionIdx, recs)

    println(s"[WORKER] received PartitionChunk from=${req.fromWorkerId}, " +
      s"partitionIdx=${req.partitionIdx}, records=${recs.size}")

    Future.successful(PartitionChunkAck(ok = true))
  }
}
