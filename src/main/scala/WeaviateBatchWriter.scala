package io.weaviate.spark

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

case class WeaviateBatchWriter(weaviateOptions: WeaviateOptions, structType: StructType) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    WeaviateDataWriterFactory(weaviateOptions, structType)
  }
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}
