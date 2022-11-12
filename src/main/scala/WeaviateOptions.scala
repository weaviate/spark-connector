package io.weaviate.spark

import io.weaviate.spark.WeaviateOptions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100

  val maxBatchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
  val host: String = config.get(WEAVIATE_HOST_CONF)
  val scheme: String = config.get(WEAVIATE_SCHEME_CONF)
  val schema: String = config.get(WEAVIATE_SCHEMA_CONF)
  val className: String = config.get(WEAVIATE_CLASSNAME_CONF)
}

object WeaviateOptions {
  val WEAVIATE_BATCH_SIZE_CONF: String = "batchSize"
  val WEAVIATE_HOST_CONF: String       = "host"
  val WEAVIATE_SCHEME_CONF: String     = "scheme"
  val WEAVIATE_SCHEMA_CONF: String     = "schema"
  val WEAVIATE_CLASSNAME_CONF: String  = "className"
}