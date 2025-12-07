package worker

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import com.google.protobuf.ByteString
import common.{Record, InputFiles}
import network.{GrpcClients, GrpcServers}
import sorting.Sampler
import sorting.v1.sort.{WorkerHello, SampleChunk}
import org.slf4j.LoggerFactory

object WorkerMain {
  private val SampleBlockSizeBytes = 4 * 1024 * 1024
  private val SampleCount          = 1000

  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    if (args.length < 5) {
      log.error(
        "Usage: worker.WorkerMain <masterHost:port> -I <inputPath> -O <outputDir>"
      )
      System.exit(1)
    }

    val masterAddr = args(0)
    val parts      = masterAddr.split(":", 2)
    if (parts.length != 2) {
      log.error(s"Invalid master address: $masterAddr (expected host:port)")
      System.exit(1)
    }
    val masterHost = parts(0)
    val masterPort = parts(1).toInt

    var inputPathOpt: Option[String]  = None
    var outputDirOpt: Option[String]  = None

    var i = 1
    while (i < args.length) {
      args(i) match {
        case "-I" if i + 1 < args.length =>
          if (inputPathOpt.nonEmpty) {
            log.error("Multiple -I not supported yet (use one inputPath per worker).")
            System.exit(1)
          }
          inputPathOpt = Some(args(i + 1))
          i += 2

        case "-O" if i + 1 < args.length =>
          if (outputDirOpt.nonEmpty) {
            log.error("Multiple -O not supported.")
            System.exit(1)
          }
          outputDirOpt = Some(args(i + 1))
          i += 2

        case other =>
          log.error(s"Unknown or malformed argument: $other")
          log.error(
            "Usage: worker.WorkerMain <masterHost:port> -I <inputPath> -O <outputDir>"
          )
          System.exit(1)
      }
    }

    val inputPath = inputPathOpt.getOrElse {
      log.error("Missing -I <inputPath>")
      System.exit(1); ""
    }

    val outputDir = outputDirOpt.getOrElse {
      log.error("Missing -O <outputDir>")
      System.exit(1); ""
    }

    WorkerState.setMasterEndpoint(masterHost, masterPort)
    WorkerState.setOutputDir(outputDir)

    val inputFiles =
      InputFiles.listInputFiles(inputPath).map(_.getAbsolutePath).toVector

    if (inputFiles.isEmpty) {
      log.error(s"No input files found at $inputPath")
      System.exit(1)
    }

    WorkerState.setInputFiles(inputFiles)

    val workerServer = GrpcServers.workerServer(0, new WorkerService)
      .start()

    val workerHost = java.net.InetAddress.getLocalHost.getHostAddress
    val workerPort = workerServer.getPort
    log.info(s"[WORKER] WorkerService listening on $workerHost:$workerPort")

    val (channel, masterStub) = GrpcClients.masterClient(masterHost, masterPort)

    val workerId = java.util.UUID.randomUUID().toString
    WorkerState.setLocalWorkerId(workerId)

    val regReq = WorkerHello(workerId, workerHost, workerPort)

    val regRep =
      Await.result(masterStub.registerWorker(regReq), 5.seconds)

    log.info(s"[WORKER] registered: ok=${regRep.ok}, assignedId=${regRep.assignedId}")

    log.info(s"[WORKER] starting streaming sample for $inputPath")
    val sampleStart = System.nanoTime()
    val samples = Sampler.sampleFromFiles(
      paths        = inputFiles,
      blockSize    = SampleBlockSizeBytes,
      targetSamples = SampleCount
    )
    val sampleMillis = (System.nanoTime() - sampleStart) / 1000000L
    WorkerState.setSampleMillis(sampleMillis)
    log.info(s"[WORKER] sampled ${samples.size} records (target=$SampleCount) in ${sampleMillis} ms")

    val outBuf = Record.encodeSeq(samples)

    val sampleReq = SampleChunk(
      workerId = workerId,
      data     = ByteString.copyFrom(outBuf)
    )

    val sampleRep = Await.result(masterStub.sendSample(sampleReq), 5.seconds)

    log.info(s"[WORKER] sample sent: ok=${sampleRep.ok}")

    channel.shutdownNow()

    log.info("[WORKER] waiting for PartitionPlan...")
    workerServer.awaitTermination()
  }
}
