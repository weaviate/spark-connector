package io.weaviate.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

case class WeaviateDataWriterFactory(weaviateOptions: WeaviateOptions, schema: StructType) extends DataWriterFactory with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    WeaviateDataWriter(weaviateOptions, schema)
  }
}
