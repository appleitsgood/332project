package worker

import io.grpc.ManagedChannelBuilder
import scala.concurrent.Await
import scala.concurrent.duration._

import sorting.v1.sort.{MasterServiceGrpc, WorkerHello}

object WorkerMain {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: worker.WorkerMain <masterHost> <masterPort>")
      System.exit(1)
    }

    val masterHost = args(0)
    val masterPort = args(1).toInt

    val channel =
      ManagedChannelBuilder
        .forAddress(masterHost, masterPort)
        .usePlaintext()
        .build()

    val stub = MasterServiceGrpc.stub(channel)

    val localHost = java.net.InetAddress.getLocalHost.getHostAddress
    val workerId  = java.util.UUID.randomUUID().toString

    val req = WorkerHello(
      workerId,
      localHost,
      0
    )

    try {
      val reply =
        Await.result(stub.registerWorker(req), 5.seconds)

      println(s"[WORKER] RegisterWorker sent to $masterHost:$masterPort")
      println(s"[WORKER] reply ok=${reply.ok}, assignedId=${reply.assignedId}")
    } catch {
      case ex: Throwable =>
        System.err.println(s"[WORKER] RegisterWorker failed: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      channel.shutdownNow()
    }
  }
}
