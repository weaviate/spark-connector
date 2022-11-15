package io.weaviate.spark

import io.weaviate.spark.WeaviateOptions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import technology.semi.weaviate.client.{Config, WeaviateClient}

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100

  val batchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
  val host: String = config.get(WEAVIATE_HOST_CONF)
  val scheme: String = config.get(WEAVIATE_SCHEME_CONF)
  val className: String = config.get(WEAVIATE_CLASSNAME_CONF)
  val id: String = config.get(WEAVIATE_ID_COLUMN_CONF)

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
  val WEAVIATE_ID_COLUMN_CONF: String  = "id"
}
