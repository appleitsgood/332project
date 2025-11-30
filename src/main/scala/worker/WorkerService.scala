package worker

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import com.google.protobuf.ByteString
import common.{PartitionPlan, Record}
import sorting.Partitioner
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, PartitionAck}

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

    println(s"[WORKER] received PartitionPlan: pivots=${pivots.size}, numPartitions=${plan.numPartitions}")

    WorkerState.getSortedRecords.foreach { recs =>
      val buckets = Partitioner.partitionByPivots(recs, pivots)
      val sizes   = buckets.map(_.size)
      println(s"[WORKER] partitioned into ${buckets.size} buckets, sizes=$sizes")
    }

    Future.successful(PartitionAck(ok = true))
  }
}
