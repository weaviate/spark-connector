ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "weaviate-spark-connector",
    idePackagePrefix := Some("io.weaviate.spark")
  )

ThisBuild / scalafixDependencies += "org.scalalint" %% "rules" % "0.1.4"

lazy val sparkVersion = "3.3.1"
lazy val weaviateClientVersion = "3.4.2"
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided,test",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1",
  "technology.semi.weaviate" % "client" % weaviateClientVersion
)
