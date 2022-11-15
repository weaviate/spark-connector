package io.weaviate.spark

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

case class WeaviateBatchWriter(weaviateOptions: WeaviateOptions, schema: StructType) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    WeaviateDataWriterFactory(weaviateOptions, schema)
  }

  override def useCommitCoordinator(): Boolean = true

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}
