package master

object MasterMain {
  def main(args: Array[String]): Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

    val expectedWorkers: Option[Int] =
      args.headOption.map(_.toInt)

    MasterState.init(expectedWorkers)

    val configuredPort = sys.props.get("port").map(_.toInt).getOrElse(0)

    val server = network.GrpcServers.masterServer(configuredPort, new MasterService.Impl)
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
}
