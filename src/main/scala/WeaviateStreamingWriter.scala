package io.weaviate.spark

import org.apache.spark.sql.connector.write.{PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

case class WeaviateStreamingWriter(weaviateOptions: WeaviateOptions, schema: StructType) extends StreamingWrite {
  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    WeaviateDataWriterFactory(weaviateOptions, schema)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
