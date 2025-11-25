package io.weaviate.spark

import io.weaviate.client6.v1.api.WeaviateClient
import io.weaviate.client6.v1.api.collections.VectorConfig
import io.weaviate.client6.v1.api.collections.vectorindex.{Flat, Hnsw, MultiVector}

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.weaviate.client6.v1.api.collections.{CollectionConfig, DataType, Property}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.Try
import java.net.{HttpURLConnection, URL}

object WeaviateDocker {
  val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "port" -> "8080", "grpc:host" -> "localhost", "grpc:port" -> "50051").asJava)
  val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
  lazy val client: WeaviateClient = weaviateOptions.getClient()
  val logger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  var retries = 10

  def start(vectorizerModule: String = "none", enableModules: String = "text2vec-openai"): Int = {
    val weaviateVersion = "1.34.0"
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

    Thread.sleep(2000L)
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
      Property.text("title"),
      Property.text("content"),
      Property.integer("wordCount")
    ) ++ additionalProperties
    println(properties.map(_.propertyName()))
    createClass("Article", "Article test class", properties)
  }

  def createClassWithText2VecOpenAIVectorizer(additionalProperties: Property*): Unit = {
    val properties = Seq(
      Property.text("title"),
      Property.text("content"),
      Property.integer("wordCount")
    ) ++ additionalProperties
    println(properties.map(_.propertyName()))

    val vectorConfig = mutable.Map[String, VectorConfig]()

    val openai = VectorConfig.text2vecOpenAi("openai")
    vectorConfig += ((openai.getKey, openai.getValue))

    createClass("Article", "Article test class with OpenAI vectorizer", properties, Some(vectorConfig.toMap))
  }

  def createBooksClass(): Unit = {
    val authorName = new Property.Builder("name", DataType.OBJECT)
      .nestedProperties(Property.text("firstName"), Property.text("lastName")).build()
    val author = new Property.Builder("author", DataType.OBJECT)
      .nestedProperties(authorName, Property.integer("age")).build()
    val properties = Seq(Property.text("title"), author)
    createClass("Books", "", properties)
  }

  def createAuthorsClass(): Unit = {
    val authors = new Property.Builder("authors", DataType.OBJECT_ARRAY)
      .nestedProperties(
        Property.text("firstName"),
        Property.text("lastName"),
        Property.bool("isAlive"),
        Property.integer("age")
      ).build()
    val properties = Seq(Property.text("genre"), authors)
    createClass("Authors", "", properties)
  }

  def createGeoClass(): Unit = {
    val properties = Seq(Property.text("title"), Property.geoCoordinates("geo"))
    createClass("GeoClass", "", properties)
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
    val properties = Seq(Property.text("title"))

    val vectorConfig = mutable.Map[String, VectorConfig]()

    val regular = VectorConfig.selfProvided("regular")
    vectorConfig += ((regular.getKey, regular.getValue))

    if (withMultiVectors) {
      val colbert = VectorConfig.selfProvided("colbert",
        vc => vc.vectorIndex(Hnsw.of(h => h.multiVector(MultiVector.of(mv => mv.enabled(true)))))
      )
      vectorConfig += ((colbert.getKey, colbert.getValue))
    }

    if (withAdditionalVectors) {
      val regular2 = VectorConfig.selfProvided("regular2", vc => vc.vectorIndex(Flat.of()))
      vectorConfig += ((regular2.getKey, regular2.getValue))

      val colbert2 = VectorConfig.selfProvided("colbert2",
        vc => vc.vectorIndex(Hnsw.of(h => h.multiVector(MultiVector.of(mv => mv.enabled(true)))))
      )
      vectorConfig += ((colbert2.getKey, colbert2.getValue))
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

  def deleteGeoClass(): Unit = {
    deleteClass("GeoClass")
  }

  private def createClass(className: String,
                          description: String,
                          properties: Seq[Property],
                          vectorConfig: Option[Map[String, VectorConfig]] = None): Unit = {
    var collectionBuilder = new CollectionConfig.Builder(className)
      .description(description)
      .properties(properties.asJava)

    if (vectorConfig.isDefined) {
      collectionBuilder = collectionBuilder.vectorConfig(vectorConfig.get.asJava)
    } else {
      collectionBuilder.vectorConfig(VectorConfig.selfProvided())
    }

    val clazz = collectionBuilder.build
    try {
      val collection = client.collections.create(clazz)
      println("Results: " + collection)
    } catch {
      case _: Throwable =>
        // retry after 0.5s
        Thread.sleep(500)
        val collection = client.collections.create(clazz)
        println("Results: " + collection)
    }
  }

  private def deleteClass(className: String): Unit = {
    client.collections.delete(className)
  }
}
