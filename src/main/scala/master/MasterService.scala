package master

import com.google.protobuf.ByteString

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import common.{PartitionPlan, Record}
import network.{GrpcClients, GrpcServers}
import sorting.Pivoter
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, WorkerEndpoint, StartMergeMsg, StartMergeAck}
import sorting.v1.sort.{MasterServiceGrpc, RegisterReply, SampleAck, SampleChunk, WorkerHello, ShuffleDone, ShuffleDoneAck}
import org.slf4j.LoggerFactory

object MasterService {

  class Impl extends MasterServiceGrpc.MasterService {
    private val log = LoggerFactory.getLogger(getClass)

    override def registerWorker(r: WorkerHello): Future[RegisterReply] = {
      val info  = WorkerInfo(r.workerId, r.host, r.port)
      val count = MasterState.addWorker(info)

      val expectedStr = MasterState.expected.map(_.toString).getOrElse("?")
      log.info(s"[REGISTER] workerId=${r.workerId} host=${r.host}:${r.port} " +
        s"($count/$expectedStr)")

      MasterState.expected.foreach { total =>
        if (count == total) {
          log.info("")
          log.info("=== ALL WORKERS REGISTERED ===")
          MasterState.allWorkers.zipWithIndex.foreach { case (w, idx) =>
            log.info(f"[$idx%02d] ${w.workerId}  ${w.host}:${w.port}")
          }
          log.info("================================")
          log.info("")
        }
      }

      Future.successful(
        RegisterReply(ok = true, assignedId = r.workerId)
      )
    }

    override def sendSample(req: SampleChunk): Future[SampleAck] = {
      val recs = Record.decodeBytes(req.data.toByteArray)
      MasterState.addSamples(req.workerId, recs)
      log.info(s"[SAMPLE] workerId=${req.workerId}, received=${recs.size} samples")

      Future {
        maybeComputePartitionPlan()
      }

      Future.successful(SampleAck(ok = true))
    }


    private def maybeComputePartitionPlan(): Unit = {
      (MasterState.expected, MasterState.samplingCompleteWorkers) match {
        case (Some(totalWorkers), sampleWorkers) if sampleWorkers == totalWorkers =>
          log.info(s"[SAMPLE] all samples collected from $sampleWorkers workers")
          if (MasterState.getStartNanos.isEmpty) {
            MasterState.markAllRegistered(System.nanoTime())
            MasterState.setStartNanos(System.nanoTime())
          }

          val samplesByWorker: Seq[Seq[Record]] =
            MasterState.allSamplesByWorker.values.toSeq

          val pivots = Pivoter.choosePivots(samplesByWorker, totalWorkers)
          log.info(s"[PIVOT] computed ${pivots.size} pivots")

          val plan = PartitionPlan.fromPivots(pivots)
          MasterState.setPartitionPlan(plan)
          log.info(s"[PLAN] numPartitions=${plan.numPartitions}")

          pivots.zipWithIndex.foreach { case (p, idx) =>
            val keyStr = new String(p.key, "US-ASCII")
            log.info(f"  pivot[$idx%02d] = $keyStr")
          }

          MasterService.sendPartitionPlan(plan)
        case _ =>
          ()
      }
    }

    override def notifyShuffleDone(req: ShuffleDone): Future[ShuffleDoneAck] = {
      MasterState.markShuffleDone(req.workerId)

      val done      = MasterState.shuffleDoneCount
      val expected  = MasterState.expected.map(_.toString).getOrElse("?")
      log.info(s"[BARRIER] shuffle done from workerId=${req.workerId} ($done/$expected)")

      if (MasterState.allShuffleDone) {
        log.info("[BARRIER] all workers finished shuffle; sending StartMerge to all workers")
        Future {
          MasterService.sendStartMerge()
        }
      }

      Future.successful(ShuffleDoneAck(ok = true))
    }
  }

  private def sendPartitionPlan(plan: PartitionPlan): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val pivotKeys: Seq[ByteString] =
      plan.pivots.map(p => ByteString.copyFrom(p.key))

    val workers = MasterState.allWorkers

    val endpoints: Seq[WorkerEndpoint] =
      workers.map { w =>
        WorkerEndpoint(
          workerId = w.workerId,
          host     = w.host,
          port     = w.port
        )
      }

    val msg = PartitionPlanMsg(
      pivotKeys = pivotKeys,
      endpoints = endpoints
    )

    workers.foreach { w =>
      log.info(s"[PLAN] sending PartitionPlan to worker ${w.workerId} at ${w.host}:${w.port}")

      val (ch, stub) = GrpcClients.workerClient(w.host, w.port)

      try {
        val ack = Await.result(stub.receivePartitionPlan(msg), 5.seconds)
        log.info(s"[PLAN] worker=${w.workerId} ack.ok=${ack.ok}")
      } catch {
        case e: Throwable =>
          log.error(s"[PLAN] failed to send plan to worker ${w.workerId}: ${e.getMessage}")
      } finally {
        ch.shutdownNow()
      }
    }
  }

  private def sendStartMerge(): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    MasterState.allWorkers.foreach { w =>
      val (ch, stub) = GrpcClients.workerClient(w.host, w.port)

      try {
        val ack = Await.result(stub.startMerge(StartMergeMsg()), 30.seconds)
        log.info(s"[BARRIER] sent StartMerge to ${w.workerId}, ack.ok=${ack.ok}")
      } catch {
        case e: Throwable =>
          log.error(s"[BARRIER] failed to send StartMerge to worker ${w.workerId}: ${e.getMessage}")
      } finally {
        ch.shutdownNow()
      }
    }

    val totalMillis = MasterState.getStartNanos.map { start =>
      (System.nanoTime() - start) / 1000000L
    }.getOrElse(-1L)
    val registeredMillis = MasterState.getAllRegisteredNanos.map { registered =>
      (System.nanoTime() - registered) / 1000000L
    }.getOrElse(-1L)

    if (totalMillis >= 0) {
      if (registeredMillis >= 0) {
        log.info(s"[MASTER] all workers finished merge; total=${totalMillis} ms since start; elapsed since all registered=${registeredMillis} ms; shutting down master.")
      } else {
        log.info(s"[MASTER] all workers finished merge; total=${totalMillis} ms; shutting down master.")
      }
    } else {
      log.info("[MASTER] all workers finished merge; shutting down master.")
    }
    System.exit(0)
  }

}
