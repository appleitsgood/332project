ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

ThisBuild / scalacOptions ++= Seq("-target:jvm-1.8")
ThisBuild / javacOptions   ++= Seq("--release", "8")

lazy val root = (project in file("."))
  .settings(
    name := "332project"
  )

