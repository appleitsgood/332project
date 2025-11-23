package master

import io.grpc.ServerBuilder
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

import sorting.v1.sort.{MasterServiceGrpc, RegisterReply, WorkerHello}

final case class WorkerInfo(workerId: String, host: String, port: Int)

object MasterState {
  @volatile private var workers: Vector[WorkerInfo] = Vector.empty
  @volatile private var expectedWorkers: Option[Int] = None

  def init(expected: Option[Int]): Unit = synchronized {
    expectedWorkers = expected
    workers = Vector.empty
  }

  def addWorker(w: WorkerInfo): Int = synchronized {
    workers = workers :+ w
    workers.size
  }

  def allWorkers: Vector[WorkerInfo] = workers
  def expected: Option[Int] = expectedWorkers
}

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
  }
}
