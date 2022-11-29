package io.weaviate.spark

import WeaviateOptions._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import technology.semi.weaviate.client.{Config, WeaviateClient}

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100
  private val DEFAULT_RETRIES = 2
  private val DEFAULT_RETRIES_BACKOFF = 2

  val batchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
  val host: String = config.get(WEAVIATE_HOST_CONF)
  val scheme: String = config.get(WEAVIATE_SCHEME_CONF)
  val className: String = config.get(WEAVIATE_CLASSNAME_CONF)
  val vector: String = config.get(WEAVIATE_VECTOR_COLUMN_CONF)
  val id: String = config.get(WEAVIATE_ID_COLUMN_CONF)
  val retries: Int = config.getInt(WEAVIATE_RETRIES_CONF, DEFAULT_RETRIES)
  val retriesBackoff: Int = config.getInt(WEAVIATE_RETRIES_BACKOFF_CONF, DEFAULT_RETRIES_BACKOFF)

  var client: WeaviateClient = _

  def getClient(): WeaviateClient = {
    if (client != null) return client
    val config = new Config(scheme, host)
    client = new WeaviateClient(config)
    client
  }
}

object WeaviateOptions {
  val WEAVIATE_BATCH_SIZE_CONF: String = "batchSize"
  val WEAVIATE_HOST_CONF: String       = "host"
  val WEAVIATE_SCHEME_CONF: String     = "scheme"
  val WEAVIATE_CLASSNAME_CONF: String  = "className"
  val WEAVIATE_VECTOR_COLUMN_CONF: String  = "vector"
  val WEAVIATE_ID_COLUMN_CONF: String  = "id"
  val WEAVIATE_RETRIES_CONF: String = "retries"
  val WEAVIATE_RETRIES_BACKOFF_CONF: String = "retriesBackoff"
}
