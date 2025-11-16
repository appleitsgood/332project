ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

ThisBuild / scalacOptions ++= Seq("-target:jvm-1.8")
ThisBuild / javacOptions   ++= Seq("--release", "8")

lazy val root = (project in file("."))
  .settings(
    name := "332project"
  )

// ========================================
// ScalaPB
// ========================================
Compile / PB.protoSources := Seq(
  (Compile / sourceDirectory).value / "proto"
)
Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)


// ========================================
// Library Dependencies
// ========================================
libraryDependencies ++= Seq(
  // protobuf runtime (필수)
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",

  // gRPC runtime
  "io.grpc" % "grpc-netty-shaded" % "1.56.1",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)