package io.weaviate.spark

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.weaviate.client.v1.schema.model.{Property, WeaviateClass}

import scala.jdk.CollectionConverters._
import scala.sys.process._

object WeaviateDocker {
  val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080").asJava)
  val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
  val client = weaviateOptions.getClient()
  val logger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  var retries = 5

  def start(): Int = {
    val weaviateVersion = "1.17.1"
    val docker_run =
      s"""docker run -d --name=weaviate-test-container-will-be-deleted
-p 8080:8080
-e QUERY_DEFAULTS_LIMIT=25
-e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true
-e DEFAULT_VECTORIZER_MODULE=none
-e CLUSTER_HOSTNAME=node1
-e PERSISTENCE_DATA_PATH=./data
semitechnologies/weaviate:$weaviateVersion"""
    val exit_code = docker_run ! logger
    exit_code
  }

  def stop(): Int = {
    "docker stop weaviate-test-container-will-be-deleted" ! logger
    "docker rm weaviate-test-container-will-be-deleted" ! logger
  }

  def createClass(additionalProperties: Property*): Unit = {
    val properties = Seq(
      Property.builder()
        .dataType(List[String]("string").asJava)
        .name("title")
        .build(),
      Property.builder()
        .dataType(List[String]("string").asJava)
        .name("content")
        .build(),
      Property.builder()
        .dataType(List[String]("int").asJava)
        .name("wordCount")
        .build(),
    ) ++ additionalProperties
    println(properties.map(_.getName))
    val clazz = WeaviateClass.builder.className("Article")
      .description("Article test class")
      .properties(properties.asJava).build

    val results = client.schema().classCreator().withClass(clazz).run
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages)
      if (retries > 1) {
        retries -= 1
        println("Retrying to create class in 1 seconds..")
        Thread.sleep(1000)
        createClass(additionalProperties: _*)
      }
    }
    println("Results: " + results.getResult)
    retries = 5
  }

  def deleteClass(): Unit = {
    val result = client.schema().classDeleter()
      .withClassName("Article")
      .run()
    if (result.hasErrors) println("Error deleting class Article " + result.getError.getMessages)
  }
}
