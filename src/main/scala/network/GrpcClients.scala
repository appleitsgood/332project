package network

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import sorting.v1.sort.{MasterServiceGrpc, WorkerServiceGrpc}

object GrpcClients {

  def newChannel(host: String, port: Int): ManagedChannel =
    ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
      .build()

  def masterClient(host: String, port: Int): (ManagedChannel, MasterServiceGrpc.MasterServiceStub) = {
    val ch   = newChannel(host, port)
    val stub = MasterServiceGrpc.stub(ch)
    (ch, stub)
  }

  def workerClient(host: String, port: Int): (ManagedChannel, WorkerServiceGrpc.WorkerServiceStub) = {
    val ch   = newChannel(host, port)
    val stub = WorkerServiceGrpc.stub(ch)
    (ch, stub)
  }
}
