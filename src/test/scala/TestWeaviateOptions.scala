package io.weaviate.spark

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class TestWeaviateOptions extends AnyFunSuite {
  test("Test fields are set correctly and batchSize is defaulted") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "pinball").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.batchSize == 100)
  }

  test("Test fields are set correctly and batchSize is set") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.batchSize == 19)
  }

  test("Test that getConnection returns the same WeaviateClient object") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080", "className" -> "pinball", "batchSize" -> "19").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val client = weaviateOptions.getClient()
    assert(client != null)
    val client2 = weaviateOptions.getClient()
    assert(client == client2)
    val weaviateOptions3: WeaviateOptions = new WeaviateOptions(options)
    val client3 = weaviateOptions3.getClient()
    assert(client != client3)
    assert(client2 != client3)
  }
}

