package master

import io.grpc.ServerBuilder
import io.grpc.ManagedChannelBuilder
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import common.{PartitionPlan, Record}
import sorting.Pivoter
import sorting.v1.sort.{WorkerServiceGrpc, PartitionPlanMsg}
import sorting.v1.sort.{MasterServiceGrpc, RegisterReply, SampleAck, SampleChunk, WorkerHello}

object MasterService {

  def main(args: Array[String]): Unit = {
    val expectedWorkers: Option[Int] =
      args.headOption.map(_.toInt)

    MasterState.init(expectedWorkers)

    val configuredPort = sys.props.get("port").map(_.toInt).getOrElse(0)

    val server = ServerBuilder
      .forPort(configuredPort)
      .addService(MasterServiceGrpc.bindService(new Impl, ExecutionContext.global))
      .build()
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
      val recs = decodeSampleBytes(req.data.toByteArray)
      MasterState.addSamples(req.workerId, recs)
      println(s"[SAMPLE] workerId=${req.workerId}, received=${recs.size} samples")

      maybeComputePartitionPlan()

      Future.successful(SampleAck(ok = true))
    }

    private def decodeSampleBytes(bytes: Array[Byte]): Vector[Record] = {
      val recSize = Record.RecordSize
      val count   = bytes.length / recSize
      val buf     = Vector.newBuilder[Record]

      var i = 0
      while (i < count) {
        val slice = java.util.Arrays.copyOfRange(bytes, i * recSize, (i + 1) * recSize)
        buf += Record.fromBytes(slice)
        i += 1
      }
      buf.result()
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
  }

  private def sendPartitionPlan(plan: PartitionPlan): Unit = {
    val pivotKeys: Seq[ByteString] =
      plan.pivots.map(p => ByteString.copyFrom(p.key))

    val msg = PartitionPlanMsg(pivotKeys = pivotKeys)

    MasterState.allWorkers.foreach { w =>
      println(s"[PLAN] sending PartitionPlan to worker ${w.workerId} at ${w.host}:${w.port}")

      val ch =
        ManagedChannelBuilder
          .forAddress(w.host, w.port)
          .usePlaintext()
          .build()

      val stub = WorkerServiceGrpc.stub(ch)

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

}
