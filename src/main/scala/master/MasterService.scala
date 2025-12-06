package master

import com.google.protobuf.ByteString

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import common.{PartitionPlan, Record}
import network.{GrpcClients, GrpcServers}
import sorting.Pivoter
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg, WorkerEndpoint, StartMergeMsg, StartMergeAck}
import sorting.v1.sort.{MasterServiceGrpc, RegisterReply, SampleAck, SampleChunk, WorkerHello, ShuffleDone, ShuffleDoneAck}

object MasterService {

  def main(args: Array[String]): Unit = {
    val expectedWorkers: Option[Int] =
      args.headOption.map(_.toInt)

    MasterState.init(expectedWorkers)

    val configuredPort = sys.props.get("port").map(_.toInt).getOrElse(0)

    val server = GrpcServers.masterServer(configuredPort, new Impl)
      .start()

    val actualPort = server.getPort
    val host       = java.net.InetAddress.getLocalHost.getHostAddress

    println(s"[MASTER] Listening on $host:$actualPort")
    expectedWorkers match {
      case Some(n) => println(s"[MASTER] Waiting for $n workers to register...")
      case None    => println(s"[MASTER] Waiting for workers to register (no fixed count).")
    }

    sys.addShutdownHook {
      println("[MASTER] Shutting down gRPC server...")
      server.shutdown()
    }

    server.awaitTermination()
  }

  class Impl extends MasterServiceGrpc.MasterService {

    override def registerWorker(r: WorkerHello): Future[RegisterReply] = {
      val info  = WorkerInfo(r.workerId, r.host, r.port)
      val count = MasterState.addWorker(info)

      val expectedStr = MasterState.expected.map(_.toString).getOrElse("?")
      println(s"[REGISTER] workerId=${r.workerId} host=${r.host}:${r.port} " +
        s"($count/$expectedStr)")

      MasterState.expected.foreach { total =>
        if (count == total) {
          println()
          println("=== ALL WORKERS REGISTERED ===")
          MasterState.allWorkers.zipWithIndex.foreach { case (w, idx) =>
            println(f"[$idx%02d] ${w.workerId}  ${w.host}:${w.port}")
          }
          println("================================")
          println()
        }
      }

      Future.successful(
        RegisterReply(ok = true, assignedId = r.workerId)
      )
    }

    override def sendSample(req: SampleChunk): Future[SampleAck] = {
      val recs = Record.decodeBytes(req.data.toByteArray)
      MasterState.addSamples(req.workerId, recs)
      println(s"[SAMPLE] workerId=${req.workerId}, received=${recs.size} samples")

      Future {
        maybeComputePartitionPlan()
      }

      Future.successful(SampleAck(ok = true))
    }


    private def maybeComputePartitionPlan(): Unit = {
      (MasterState.expected, MasterState.samplingCompleteWorkers) match {
        case (Some(totalWorkers), sampleWorkers) if sampleWorkers == totalWorkers =>
          println(s"[SAMPLE] all samples collected from $sampleWorkers workers")

          val samplesByWorker: Seq[Seq[Record]] =
            MasterState.allSamplesByWorker.values.toSeq

          val pivots = Pivoter.choosePivots(samplesByWorker, totalWorkers)
          println(s"[PIVOT] computed ${pivots.size} pivots")

          val plan = PartitionPlan.fromPivots(pivots)
          MasterState.setPartitionPlan(plan)
          println(s"[PLAN] numPartitions=${plan.numPartitions}")

          pivots.zipWithIndex.foreach { case (p, idx) =>
            val keyStr = new String(p.key, "US-ASCII")
            println(f"  pivot[$idx%02d] = $keyStr")
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
      println(s"[BARRIER] shuffle done from workerId=${req.workerId} ($done/$expected)")

      if (MasterState.allShuffleDone) {
        println("[BARRIER] all workers finished shuffle; sending StartMerge to all workers")
        Future {
          MasterService.sendStartMerge()
        }
      }

      Future.successful(ShuffleDoneAck(ok = true))
    }
  }

  private def sendPartitionPlan(plan: PartitionPlan): Unit = {
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
      println(s"[PLAN] sending PartitionPlan to worker ${w.workerId} at ${w.host}:${w.port}")

      val (ch, stub) = GrpcClients.workerClient(w.host, w.port)

      try {
        val ack = Await.result(stub.receivePartitionPlan(msg), 5.seconds)
        println(s"[PLAN] worker=${w.workerId} ack.ok=${ack.ok}")
      } catch {
        case e: Throwable =>
          println(s"[PLAN] failed to send plan to worker ${w.workerId}: ${e.getMessage}")
      } finally {
        ch.shutdownNow()
      }
    }
  }

  private def sendStartMerge(): Unit = {
    MasterState.allWorkers.foreach { w =>
      val (ch, stub) = GrpcClients.workerClient(w.host, w.port)

      try {
        val ack = Await.result(stub.startMerge(StartMergeMsg()), 30.seconds)
        println(s"[BARRIER] sent StartMerge to ${w.workerId}, ack.ok=${ack.ok}")
      } catch {
        case e: Throwable =>
          println(s"[BARRIER] failed to send StartMerge to worker ${w.workerId}: ${e.getMessage}")
      } finally {
        ch.shutdownNow()
      }
    }

    println("[MASTER] all workers finished merge; shutting down master.")
    System.exit(0)
  }

}
