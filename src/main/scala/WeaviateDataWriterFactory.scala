package io.weaviate.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

case class WeaviateDataWriterFactory(weaviateOptions: WeaviateOptions) extends DataWriterFactory with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = WeaviateDataWriter(weaviateOptions)
}
