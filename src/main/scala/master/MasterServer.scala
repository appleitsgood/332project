package master

import io.grpc.ServerBuilder
import sorting.v1.sort.{MasterServiceGrpc, RegisterReply, WorkerHello}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object MasterServer {
  def main(args: Array[String]): Unit = {
    val port   = sys.props.get("port").map(_.toInt).getOrElse(7777)
    val server = ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(new Impl, global))
      .build()
      .start()

    println(s"MASTER LISTEN port=$port")
    sys.addShutdownHook(server.shutdown())
    server.awaitTermination()
  }

  class Impl extends MasterServiceGrpc.MasterService {
    override def registerWorker(r: WorkerHello): Future[RegisterReply] = {
      println(s"REGISTERED workerId=${r.workerId} host=${r.host}:${r.port}")
      Future.successful(RegisterReply(ok = true, assignedId = r.workerId))
    }
  }
}
