package worker

import io.grpc.{ManagedChannelBuilder, ServerBuilder}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import com.google.protobuf.ByteString
import common.Record
import sorting.{Sorter, Sampler}
import sorting.v1.sort.{
  MasterServiceGrpc,
  WorkerServiceGrpc,
  WorkerHello,
  SampleChunk
}

object WorkerMain {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: worker.WorkerMain <masterHost> <masterPort> <inputPath>")
      System.exit(1)
    }

    val masterHost = args(0)
    val masterPort = args(1).toInt
    val inputPath  = args(2)

    val workerServer =
      ServerBuilder
        .forPort(0)
        .addService(
          WorkerServiceGrpc.bindService(
            new WorkerService,
            ExecutionContext.global
          )
        )
        .build()
        .start()

    val workerHost = java.net.InetAddress.getLocalHost.getHostAddress
    val workerPort = workerServer.getPort
    println(s"[WORKER] WorkerService listening on $workerHost:$workerPort")

    val channel =
      ManagedChannelBuilder
        .forAddress(masterHost, masterPort)
        .usePlaintext()
        .build()

    val masterStub = MasterServiceGrpc.stub(channel)

    val workerId = java.util.UUID.randomUUID().toString
    val regReq   = WorkerHello(workerId, workerHost, workerPort)

    val regRep =
      Await.result(masterStub.registerWorker(regReq), 5.seconds)

    println(s"[WORKER] registered: ok=${regRep.ok}, assignedId=${regRep.assignedId}")


    println(s"[WORKER] starting local sort for $inputPath")
    val sorted = Sorter.localSort(inputPath)
    println(s"[WORKER] local sort done, records=${sorted.size}")

    val samples = Sampler.takeUniformSamples(sorted, targetSamples = 1000)
    println(s"[WORKER] sampled ${samples.size} records")

    val recSize = Record.RecordSize
    val outBuf  = new Array[Byte](samples.size * recSize)
    var i       = 0
    samples.foreach { r =>
      val bytes = Record.toBytes(r)
      System.arraycopy(bytes, 0, outBuf, i * recSize, recSize)
      i += 1
    }

    val sampleReq = SampleChunk(
      workerId = workerId,
      data     = ByteString.copyFrom(outBuf)
    )

    val sampleRep =
      Await.result(masterStub.sendSample(sampleReq), 5.seconds)

    println(s"[WORKER] sample sent: ok=${sampleRep.ok}")

    channel.shutdownNow()

    println("[WORKER] waiting for PartitionPlan...")
    workerServer.awaitTermination()
  }
}
