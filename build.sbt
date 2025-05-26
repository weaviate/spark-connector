import ReleaseTransformations._

resolvers += Resolver.mavenLocal

ThisBuild / scalaVersion := "2.12.18"

crossScalaVersions := Seq("2.12.18", "2.13.12")

lazy val root = (project in file("."))
  .settings(
    name := "spark-connector",
    idePackagePrefix := Some("io.weaviate.spark")
  )

ThisBuild / scalafixDependencies += "org.scalalint" %% "rules" % "0.2.1" % "runtime"

lazy val sparkVersion = "3.5.5"
lazy val grpcNettyShadedVersion = "1.72.0"
lazy val weaviateClientVersion = "5.2.1"
lazy val scalaCollectionCompatVersion = "2.13.0"
lazy val scalatestVersion = "3.2.19"
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided,test",
  "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatVersion,
  "io.weaviate" % "client" % weaviateClientVersion,
  "io.grpc" % "grpc-netty-shaded" % grpcNettyShadedVersion
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shade_proto.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade_googlecommon.@1").inAll
)

ThisBuild / organization := "io.weaviate"
ThisBuild / organizationName := "Weaviate B.V."
ThisBuild / organizationHomepage := Some(url("https://weaviate.io"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/weaviate/spark-connector/tree/main"),
    "scm:git@github.com:weaviate/spark-connector.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "aliszka",
    name = "Andrzej Liszka",
    email = "andrzej@weaviate.io",
    url = url("https://github.com/aliszka")
  ),
  Developer(
    id = "samos123",
    name = "Sam Stoelinga",
    email = "",
    url = url("https://github.com/samos123")
  ),
  Developer(
    id = "sam-h-bean",
    name = "Sam Bean",
    email = "",
    url = url("https://github.com/sam-h-bean")
  ),
)

ThisBuild / description := "Weaviate Spark Connector to use in Spark ETLs to populate a Weaviate vector database."
ThisBuild / licenses := List(
  "Weaviate B.V. License" -> new URL("https://github.com/weaviate/spark-connector/blob/main/LICENSE")
)
ThisBuild / homepage := Some(url("https://github.com/weaviate/spark-connector"))

// Fix for "deduplicate: different file contents found in the following" error
ThisBuild / assemblyMergeStrategy  := {
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

Test / parallelExecution := false

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ => false }
publishMavenStyle := true
publishTo := sonatypePublishToBundle.value
sonatypeCredentialHost := "s01.oss.sonatype.org"
sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
sonatypeProfileName := "io.weaviate"

// Custom release process
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
//  publishArtifacts,   // done by CI on tag push
  setNextVersion,
  commitNextVersion,
//  pushChanges         // done manually
)