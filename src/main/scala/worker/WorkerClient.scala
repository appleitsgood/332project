package worker

import network.GrpcClients
import sorting.v1.sort.{MasterServiceGrpc, WorkerHello}

import scala.concurrent.Await
import scala.concurrent.duration._

object WorkerClient {
  def main(args: Array[String]): Unit = {
    val host = if (args.length > 0) args(0) else "127.0.0.1"
    val port = if (args.length > 1) args(1).toInt else 7777

    val (ch, stub) = GrpcClients.masterClient(host, port)

    val req  = WorkerHello(workerId = java.util.UUID.randomUUID().toString,
      host = "worker.local", port = 0)
    val rep  = Await.result(stub.registerWorker(req), 5.seconds)
    println(s"REGISTER_REPLY ok=${rep.ok} assigned=${rep.assignedId}")
    ch.shutdownNow()
  }
}
