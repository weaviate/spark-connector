package io.weaviate.spark

import io.weaviate.spark.WeaviateOptions.WEAVIATE_BATCH_SIZE_CONF
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class WeaviateOptions(config: CaseInsensitiveStringMap) extends Serializable {
  private val DEFAULT_BATCH_SIZE = 100

  val maxBatchSize: Int =
    config
      .getInt(WEAVIATE_BATCH_SIZE_CONF, DEFAULT_BATCH_SIZE)
}

object WeaviateOptions {
  val WEAVIATE_BATCH_SIZE_CONF: String   = "batchSize"
}