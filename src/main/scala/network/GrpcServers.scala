package network

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.ExecutionContext
import sorting.v1.sort.{MasterServiceGrpc, WorkerServiceGrpc}

object GrpcServers {

  def masterServer(port: Int, impl: MasterServiceGrpc.MasterService)
                  (implicit ec: ExecutionContext): Server =
    ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(impl, ec))
      .build()

  def workerServer(port: Int, impl: WorkerServiceGrpc.WorkerService)
                  (implicit ec: ExecutionContext): Server =
    ServerBuilder
      .forPort(port)
      .addService(WorkerServiceGrpc.bindService(impl, ec))
      .build()
}