ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "weaviate-spark-connector",
    idePackagePrefix := Some("io.weaviate.spark")
  )

ThisBuild / scalafixDependencies += "org.scalalint" %% "rules" % "0.1.4"

lazy val sparkVersion = "3.2.0"
lazy val weaviateClientVersion = "3.2.0"
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided,test",
  "technology.semi.weaviate" % "client" % weaviateClientVersion % "provided,test"
)
