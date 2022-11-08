package io.weaviate.spark

import org.apache.spark.sql.connector.write.{BatchWrite, Write}

case class WeaviateWrite() extends Write with Serializable {
  override def toBatch: BatchWrite = WeaviateBatchWriter()
}
