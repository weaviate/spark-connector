package io.weaviate.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.types.StructType

case class WeaviateStreamingDataWriterFactory(weaviateOptions: WeaviateOptions, schema: StructType)
  extends StreamingDataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    WeaviateDataWriter(weaviateOptions, schema)
  }
}
