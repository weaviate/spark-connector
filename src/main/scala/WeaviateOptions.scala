package io.weaviate.spark

import WeaviateOptions._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.weaviate.client6.v1.api.{WeaviateClient, Config, Authentication}
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel

import scala.jdk.CollectionConverters._
import io.weaviate.client6.v1.api.collections.CollectionConfig

import scala.util.Properties

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100
  private val DEFAULT_RETRIES = 2
  private val DEFAULT_RETRIES_BACKOFF = 2
  private val DEFAULT_TIMEOUT_SECONDS = 60
  private val DATABRICKS_RUNTIME_VERSION = "DATABRICKS_RUNTIME_VERSION"

  val batchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
  val className: String = config.get(WEAVIATE_CLASSNAME_CONF)
  val scheme: String = config.get(WEAVIATE_SCHEME_CONF)
  val host: String = config.get(WEAVIATE_HOST_CONF)
  val port: Int = config.getInt(WEAVIATE_HOST_PORT, 80)
  val grpcSecured: Boolean = config.getBoolean(WEAVIATE_GRPC_SECURED, false)
  val grpcHost: String = config.getOrDefault(WEAVIATE_GRPC_HOST, host)
  val grpcPort: Int = config.getInt(WEAVIATE_GRPC_PORT, 50051)

  val tenant: String = config.getOrDefault(WEAVIATE_TENANT_CONF, null)
  val vector: String = config.get(WEAVIATE_VECTOR_COLUMN_CONF)
  val id: String = config.get(WEAVIATE_ID_COLUMN_CONF)
  val retries: Int = config.getInt(WEAVIATE_RETRIES_CONF, DEFAULT_RETRIES)
  val retriesBackoff: Int = config.getInt(WEAVIATE_RETRIES_BACKOFF_CONF, DEFAULT_RETRIES_BACKOFF)
  val timeout: Int = config.getInt(WEAVIATE_TIMEOUT, DEFAULT_TIMEOUT_SECONDS)
  val oidcUsername: String = config.getOrDefault(WEAVIATE_OIDC_USERNAME, "")
  val oidcPassword: String = config.getOrDefault(WEAVIATE_OIDC_PASSWORD, "")
  val oidcClientId: String = config.getOrDefault(WEAVIATE_OIDC_CLIENT_ID, "")
  val oidcClientSecret: String = config.getOrDefault(WEAVIATE_OIDC_CLIENT_SECRET, "")
  val oidcAccessToken: String = config.getOrDefault(WEAVIATE_OIDC_ACCESS_TOKEN, "")
  val oidcAccessTokenLifetime: Long = config.getLong(WEAVIATE_OIDC_ACCESS_TOKEN_LIFETIME, 0)
  val oidcScopes: List[String] = List.empty
  val oidcRefreshToken: String = config.getOrDefault(WEAVIATE_OIDC_REFRESH_TOKEN, "")
  val apiKey: String = config.getOrDefault(WEAVIATE_API_KEY, "")
  val consistencyLevel: ConsistencyLevel = {
    val value = config.getOrDefault(WEAVIATE_CONSISTENCY_LEVEL, "")
    try {
      if (value != "") ConsistencyLevel.valueOf(value) else null
    } catch {
      case e: Exception =>
        throw WeaviateOptionsError(s"Invalid consistency level: ${value}")
    }
  }

  var headers: Map[String, String] = Map()
  config.forEach((option, value) => {
    if (option.startsWith(WEAVIATE_HEADER_PREFIX)) {
      headers += (option.replace(WEAVIATE_HEADER_PREFIX, "") -> value)
    }
  })
  Properties.envOrNone(DATABRICKS_RUNTIME_VERSION) match {
    case Some(value) if value.nonEmpty => headers += ("X-Databricks-User-Agent" -> "weaviate+spark_connector")
    case None =>
  }

  var vectors: Map[String, String] = Map()
  config.forEach((option, value) => {
    if (option.startsWith(WEAVIATE_VECTORS_COLUMN_PREFIX)) {
      vectors += (value -> option.replace(WEAVIATE_VECTORS_COLUMN_PREFIX, ""))
    }
  })

  var multiVectors: Map[String, String] = Map()
  config.forEach((option, value) => {
    if (option.startsWith(WEAVIATE_MULTIVECTORS_COLUMN_PREFIX)) {
      multiVectors += (value -> option.replace(WEAVIATE_MULTIVECTORS_COLUMN_PREFIX, ""))
    }
  })

  private lazy val client: WeaviateClient = createClient()

  private def createClient(): WeaviateClient = {
    val config = new Config.Custom()
      .scheme(scheme).httpHost(host).httpPort(port)
      .grpcHost(grpcHost).grpcPort(grpcPort)
      .timeout(timeout)
      .setHeaders(headers.asJava)
    if (oidcUsername.trim().nonEmpty && oidcPassword.trim().nonEmpty) {
      config.authentication(Authentication.resourceOwnerPassword(oidcUsername, oidcPassword, oidcScopes.asJava))
    } else if (oidcClientSecret.trim().nonEmpty) {
      config.authentication(Authentication.clientCredentials(oidcClientSecret, oidcScopes.asJava))
    } else if (oidcAccessToken.trim().nonEmpty) {
      config.authentication(Authentication.bearerToken(oidcAccessToken, oidcRefreshToken, oidcAccessTokenLifetime))
    } else if (apiKey.trim().nonEmpty) {
      config.authentication(Authentication.apiKey(apiKey))
    }
    new WeaviateClient(config.build())
  }

  def getClient(): WeaviateClient = {
    client
  }

  def getCollectionConfig(): CollectionConfig = {
    getClient().collections.getConfig(className).orElse(null)
  }
}

object WeaviateOptions {
  val WEAVIATE_BATCH_SIZE_CONF: String = "batchSize"
  val WEAVIATE_CLASSNAME_CONF: String  = "className"
  val WEAVIATE_SCHEME_CONF: String     = "scheme"
  val WEAVIATE_HOST_CONF: String       = "host"
  val WEAVIATE_HOST_PORT: String       = "port"
  val WEAVIATE_GRPC_SECURED: String = "grpc:secured"
  val WEAVIATE_GRPC_HOST: String = "grpc:host"
  val WEAVIATE_GRPC_PORT: String = "grpc:port"
  val WEAVIATE_TENANT_CONF: String  = "tenant"
  val WEAVIATE_VECTOR_COLUMN_CONF: String  = "vector"
  val WEAVIATE_VECTORS_COLUMN_PREFIX: String  = "vectors:"
  val WEAVIATE_MULTIVECTORS_COLUMN_PREFIX: String  = "multivectors:"
  val WEAVIATE_ID_COLUMN_CONF: String  = "id"
  val WEAVIATE_RETRIES_CONF: String = "retries"
  val WEAVIATE_RETRIES_BACKOFF_CONF: String = "retriesBackoff"
  val WEAVIATE_TIMEOUT: String = "timeout"
  val WEAVIATE_OIDC_USERNAME: String = "oidc:username"
  val WEAVIATE_OIDC_PASSWORD: String = "oidc:password"
  val WEAVIATE_OIDC_CLIENT_ID: String = "oidc:clientId"
  val WEAVIATE_OIDC_CLIENT_SECRET: String = "oidc:clientSecret"
  val WEAVIATE_OIDC_ACCESS_TOKEN: String = "oidc:accessToken"
  val WEAVIATE_OIDC_ACCESS_TOKEN_LIFETIME: String = "oidc:accessTokenLifetime"
  val WEAVIATE_OIDC_REFRESH_TOKEN: String = "oidc:refreshToken"
  val WEAVIATE_API_KEY: String = "apiKey"
  val WEAVIATE_HEADER_PREFIX: String = "header:"
  val WEAVIATE_CONSISTENCY_LEVEL: String = "consistencyLevel"
}
