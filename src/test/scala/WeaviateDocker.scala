package io.weaviate.spark

import io.weaviate.client.v1.misc.model.{MultiVectorConfig, VectorIndexConfig}
import io.weaviate.client.v1.schema.model.Property.NestedProperty
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.weaviate.client.v1.schema.model.{DataType, Property, WeaviateClass}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process._

import scala.util.Try
import java.net.{URL, HttpURLConnection}

object WeaviateDocker {
  val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080").asJava)
  val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
  val client = weaviateOptions.getClient()
  val logger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  var retries = 10

  def start(vectorizerModule: String = "none", enableModules: String = "text2vec-openai"): Int = {
    val weaviateVersion = "1.30.6"
    val docker_run =
      s"""docker run -d --name=weaviate-test-container-will-be-deleted
-p 8080:8080
-p 50051:50051
-e QUERY_DEFAULTS_LIMIT=25
-e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true
-e DEFAULT_VECTORIZER_MODULE=$vectorizerModule
-e ENABLE_MODULES=$enableModules
-e CLUSTER_HOSTNAME=weaviate-0
-e RAFT_BOOTSTRAP_EXPECT=1
-e RAFT_JOIN=weaviate-0
-e PERSISTENCE_DATA_PATH=./data
semitechnologies/weaviate:$weaviateVersion"""
    val exit_code = docker_run ! logger
    exit_code
  }

  def stop(): Int = {
    "docker stop weaviate-test-container-will-be-deleted" ! logger
    "docker rm weaviate-test-container-will-be-deleted" ! logger
  }

  def checkReadyEndpoint(): Boolean = {
    def checkReadinessProbe: Boolean = {
      Try {
        val url = new URL("http://localhost:8080/v1/.well-known/ready")
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]
        try {
          connection.setRequestMethod("GET")
          connection.setConnectTimeout(1000) // 1s timeout
          connection.setReadTimeout(1000)
          connection.getResponseCode == 200
        } finally {
          connection.disconnect()
        }
      }.getOrElse(false)
    }

    val maxAttempts = 10
    for (_ <- 1 to maxAttempts) {
      if (checkReadinessProbe) {
        return true
      }
      Thread.sleep(1000L)
    }
    false
  }

  def createClass(additionalProperties: Property*): Unit = {
    val properties = Seq(
      Property.builder()
        .dataType(List[String]("text").asJava)
        .name("title")
        .build(),
      Property.builder()
        .dataType(List[String]("text").asJava)
        .name("content")
        .build(),
      Property.builder()
        .dataType(List[String]("int").asJava)
        .name("wordCount")
        .build(),
    ) ++ additionalProperties
    println(properties.map(_.getName))
    createClass("Article", "Article test class", properties)
  }

  def createBooksClass(): Unit = {
    val properties = Seq(
      Property.builder()
        .name("title")
        .dataType(List[String](DataType.TEXT).asJava)
        .build(),
      Property.builder()
        .name("author")
        .dataType(List[String](DataType.OBJECT).asJava)
        .nestedProperties(List[NestedProperty](
          Property.NestedProperty.builder()
            .name("name")
            .dataType(List[String](DataType.OBJECT).asJava)
            .nestedProperties(List[NestedProperty](
              Property.NestedProperty.builder()
                .name("firstName")
                .dataType(List[String](DataType.TEXT).asJava)
                .build(),
              Property.NestedProperty.builder()
                .name("lastName")
                .dataType(List[String](DataType.TEXT).asJava)
                .build()
            ).asJava)
        .build(),
          Property.NestedProperty.builder()
            .name("age")
            .dataType(List[String](DataType.INT).asJava)
            .build()
        ).asJava).build()
    )
    createClass("Books", "", properties)
  }

  def createAuthorsClass(): Unit = {
    val properties = Seq(
      Property.builder()
        .name("genre")
        .dataType(List[String](DataType.TEXT).asJava)
        .build(),
      Property.builder()
        .name("authors")
        .dataType(List[String](DataType.OBJECT_ARRAY).asJava)
        .nestedProperties(List[NestedProperty](
          Property.NestedProperty.builder()
            .name("firstName")
            .dataType(List[String](DataType.TEXT).asJava)
            .build(),
          Property.NestedProperty.builder()
            .name("lastName")
            .dataType(List[String](DataType.TEXT).asJava)
            .build(),
          Property.NestedProperty.builder()
            .name("isAlive")
            .dataType(List[String](DataType.BOOLEAN).asJava)
            .build(),
          Property.NestedProperty.builder()
            .name("age")
            .dataType(List[String](DataType.INT).asJava)
            .build()
        ).asJava).build()
    )
    createClass("Authors", "", properties)
  }

  def createRegularVectorsClass(): Unit = {
    createNamedVectorsClass("RegularVectors", false)
  }

  def createMultiVectorsClass(): Unit = {
    createNamedVectorsClass("MultiVectors", true)
  }

  def createMixedVectorsClass(): Unit = {
    createNamedVectorsClass("MixedVectors", withMultiVectors = true, withAdditionalVectors = true)
  }

  private def createNamedVectorsClass(name: String, withMultiVectors: Boolean, withAdditionalVectors: Boolean = false): Unit = {
    val properties = Seq(
      Property.builder()
        .name("title")
        .dataType(List[String](DataType.TEXT).asJava)
        .build(),
    )

    val vectorConfig = mutable.Map[String, WeaviateClass.VectorConfig]()

    val regular = WeaviateClass.VectorConfig.builder()
      .vectorizer(Map[String, Object]("none" -> new Object()).asJava)
      .vectorIndexType("hnsw")
      .build()

    vectorConfig += ("regular" -> regular)

    if (withMultiVectors) {
      val colbert = WeaviateClass.VectorConfig.builder()
        .vectorizer(Map[String, Object]("none" -> new Object()).asJava)
        .vectorIndexConfig(VectorIndexConfig.builder()
          .multiVector(MultiVectorConfig.builder().build())
          .build())
        .vectorIndexType("hnsw")
        .build()

      vectorConfig += ("colbert" -> colbert)
    }

    if (withAdditionalVectors) {
      val regular2 = WeaviateClass.VectorConfig.builder()
        .vectorizer(Map[String, Object]("none" -> new Object()).asJava)
        .vectorIndexType("flat")
        .build()

      vectorConfig += ("regular2" -> regular2)

      val colbert2 = WeaviateClass.VectorConfig.builder()
        .vectorizer(Map[String, Object]("none" -> new Object()).asJava)
        .vectorIndexConfig(VectorIndexConfig.builder()
          .multiVector(MultiVectorConfig.builder().build())
          .build())
        .vectorIndexType("hnsw")
        .build()

      vectorConfig += ("colbert2" -> colbert2)
    }

    createClass(name, "", properties, Some(vectorConfig.toMap))
  }

  def deleteClass(): Unit = {
    deleteClass("Article")
  }

  def deleteBooksClass(): Unit = {
    deleteClass("Books")
  }

  def deleteAuthorsClass(): Unit = {
    deleteClass("Authors")
  }

  def deleteRegularVectorsClass(): Unit = {
    deleteClass("RegularVectors")
  }

  def deleteMultiVectorsClass(): Unit = {
    deleteClass("MultiVectors")
  }

  def deleteMixedVectorsClass(): Unit = {
    deleteClass("MixedVectors")
  }

  private def createClass(className: String, description: String, properties: Seq[Property], vectorConfig: Option[Map[String, WeaviateClass.VectorConfig]] = None): Unit = {
    var clazzBuilder = WeaviateClass.builder.className(className)
      .description(description)
      .properties(properties.asJava)

    if (vectorConfig.isDefined) {
      clazzBuilder = clazzBuilder.vectorConfig(vectorConfig.get.asJava)
    }

    val clazz = clazzBuilder.build
    val results = client.schema().classCreator().withClass(clazz).run
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages)
      if (retries > 1) {
        retries -= 1
        println("Retrying to create class in 0.1 seconds..")
        Thread.sleep(100)
        createClass(className, description, properties)
      }
    }
    println("Results: " + results.getResult)
    retries = 10
  }

  private def deleteClass(className: String): Unit = {
    val result = client.schema().classDeleter()
      .withClassName(className)
      .run()
    if (result.hasErrors) println(s"Error deleting class ${className} ${result.getError.getMessages}")
  }
}
