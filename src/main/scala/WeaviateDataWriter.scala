package io.weaviate.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

case class WeaviateDataWriter() extends DataWriter[InternalRow] with Serializable {
  override def write(record: InternalRow): Unit = ???
  override def close(): Unit = ???
  override def commit(): WriterCommitMessage = ???
  override def abort(): Unit = ???
}
