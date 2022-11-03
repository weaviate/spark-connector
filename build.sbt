ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "weaviate-spark-connector",
    idePackagePrefix := Some("io.weaviate.spark")
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
ThisBuild / scalafixDependencies += "org.scalalint" %% "rules" % "0.1.4"
