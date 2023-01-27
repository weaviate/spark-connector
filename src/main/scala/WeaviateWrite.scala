package io.weaviate.spark

import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType

case class WeaviateWrite(weaviateOptions: WeaviateOptions, schema: StructType) extends Write with Serializable {
  override def toBatch: BatchWrite = WeaviateBatchWriter(weaviateOptions, schema)
  override def toStreaming: StreamingWrite = WeaviateStreamingWriter(weaviateOptions, schema)
}
