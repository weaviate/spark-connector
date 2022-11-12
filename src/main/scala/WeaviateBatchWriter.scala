package io.weaviate.spark

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

case class WeaviateBatchWriter(weaviateOptions: WeaviateOptions) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = WeaviateDataWriterFactory(weaviateOptions)
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}
