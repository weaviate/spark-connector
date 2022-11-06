package io.weaviate.spark

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.slf4j.LoggerFactory

case class WeaviateBatchWriter() extends BatchWrite {
  private val logger = LoggerFactory.getLogger(getClass)
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }
}
