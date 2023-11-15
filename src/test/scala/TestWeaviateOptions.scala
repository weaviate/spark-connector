package io.weaviate.spark

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class TestWeaviateOptions extends AnyFunSuite {
  test("Test fields are set correctly with a Tenant") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "pinball", "tenant" -> "TenantA").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == "TenantA")
    assert(weaviateOptions.batchSize == 100)
    assert(weaviateOptions.retries == 2)
    assert(weaviateOptions.retriesBackoff == 2)
  }

  test("Test fields are set correctly, batchSize and retries is defaulted") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "pinball").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 100)
    assert(weaviateOptions.retries == 2)
    assert(weaviateOptions.retriesBackoff == 2)
  }

  test("Test fields are set correctly, batchSize and retries is set") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
  }

  test("Test fields are set correctly, timeout is set") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "timeout" -> "120").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 120)
  }

  test("Test fields are set correctly, timeout is defaulted") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
  }

  test("Test OIDC fields are set correctly (username, password)") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "oidc:username" -> "user", "oidc:password" -> "pass").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.oidcUsername == "user")
    assert(weaviateOptions.oidcPassword == "pass")
    assert(weaviateOptions.oidcClientSecret == "")
    assert(weaviateOptions.oidcAccessToken == "")
    assert(weaviateOptions.oidcAccessTokenLifetime == 0)
    assert(weaviateOptions.oidcRefreshToken == "")
  }

  test("Test OIDC fields are set correctly (clientSecret)") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "oidc:clientSecret" -> "secret").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.oidcUsername == "")
    assert(weaviateOptions.oidcPassword == "")
    assert(weaviateOptions.oidcClientSecret == "secret")
    assert(weaviateOptions.oidcAccessToken == "")
    assert(weaviateOptions.oidcAccessTokenLifetime == 0)
    assert(weaviateOptions.oidcRefreshToken == "")
  }

  test("Test OIDC fields are set correctly (accessToken)") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "oidc:accessToken" -> "accessToken").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.oidcUsername == "")
    assert(weaviateOptions.oidcPassword == "")
    assert(weaviateOptions.oidcClientSecret == "")
    assert(weaviateOptions.oidcAccessToken == "accessToken")
    assert(weaviateOptions.oidcAccessTokenLifetime == 0)
    assert(weaviateOptions.oidcRefreshToken == "")
  }

  test("Test OIDC fields are set correctly (accessToken, accessTokenLifetime, refreshToken)") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "oidc:accessToken" -> "accessToken", "oidc:accessTokenLifetime" -> "100", "oidc:refreshToken" -> "refreshToken").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.oidcUsername == "")
    assert(weaviateOptions.oidcPassword == "")
    assert(weaviateOptions.oidcClientSecret == "")
    assert(weaviateOptions.oidcAccessToken == "accessToken")
    assert(weaviateOptions.oidcAccessTokenLifetime == 100)
    assert(weaviateOptions.oidcRefreshToken == "refreshToken")
  }

  test("Test API Key is set correctly") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "apiKey" -> "apiKey").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.apiKey == "apiKey")
  }

  test("Test that headers are added correctly") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "oidc:accessToken" -> "accessToken", "oidc:accessTokenLifetime" -> "100", "oidc:refreshToken" -> "refreshToken",
        "header:X-OpenAI-Api-Key" -> "OPENAI_KEY", "header:X-Cohere-Api-Key" -> "COHERE_KEY").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.tenant == null)
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.oidcUsername == "")
    assert(weaviateOptions.oidcPassword == "")
    assert(weaviateOptions.oidcClientSecret == "")
    assert(weaviateOptions.oidcAccessToken == "accessToken")
    assert(weaviateOptions.oidcAccessTokenLifetime == 100)
    assert(weaviateOptions.oidcRefreshToken == "refreshToken")
    assert(weaviateOptions.headers.size == 2)
    assert(weaviateOptions.headers.asJava.get("x-openai-api-key") == "OPENAI_KEY")
    assert(weaviateOptions.headers.asJava.get("x-cohere-api-key") == "COHERE_KEY")
  }

  test("Test enable gRPC is set correctly") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "apiKey" -> "apiKey", "grpc:secured" -> "true", "grpc:host" -> "localhost:50051").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)

    assert(weaviateOptions.scheme == "http")
    assert(weaviateOptions.host == "localhost")
    assert(weaviateOptions.className == "pinball")
    assert(weaviateOptions.batchSize == 19)
    assert(weaviateOptions.retries == 5)
    assert(weaviateOptions.retriesBackoff == 5)
    assert(weaviateOptions.timeout == 60)
    assert(weaviateOptions.apiKey == "apiKey")
    assert(weaviateOptions.grpcSecured)
    assert(weaviateOptions.grpcHost == "localhost:50051")

    val options2: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "apiKey" -> "apiKey", "grpc:secured" -> "False").asJava)
    val weaviateOptions2: WeaviateOptions = new WeaviateOptions(options2)

    assert(weaviateOptions2.scheme == "http")
    assert(weaviateOptions2.host == "localhost")
    assert(weaviateOptions2.className == "pinball")
    assert(weaviateOptions2.batchSize == 19)
    assert(weaviateOptions2.retries == 5)
    assert(weaviateOptions2.retriesBackoff == 5)
    assert(weaviateOptions2.timeout == 60)
    assert(weaviateOptions2.apiKey == "apiKey")
    assert(!weaviateOptions2.grpcSecured)
    assert(weaviateOptions2.grpcHost == null)

    val options3: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost",
        "className" -> "pinball", "batchSize" -> "19", "retries" -> "5", "retriesBackoff" -> "5",
        "apiKey" -> "apiKey").asJava)
    val weaviateOptions3: WeaviateOptions = new WeaviateOptions(options3)

    assert(weaviateOptions3.scheme == "http")
    assert(weaviateOptions3.host == "localhost")
    assert(weaviateOptions3.className == "pinball")
    assert(weaviateOptions3.batchSize == 19)
    assert(weaviateOptions3.retries == 5)
    assert(weaviateOptions3.retriesBackoff == 5)
    assert(weaviateOptions3.timeout == 60)
    assert(weaviateOptions3.apiKey == "apiKey")
    assert(!weaviateOptions3.grpcSecured)
    assert(weaviateOptions3.grpcHost == null)
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

  test("Test valid consistency level") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080", "className" -> "pinball", "consistencyLevel" -> "ALL").asJava)

    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    assert(weaviateOptions.consistencyLevel == "ALL")
  }

  test("Test invalid consistency level") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080", "className" -> "pinball", "consistencyLevel" -> "EVENTUAL").asJava)

    assertThrows[WeaviateOptionsError] {
      new WeaviateOptions(options)
    }
  }
}

