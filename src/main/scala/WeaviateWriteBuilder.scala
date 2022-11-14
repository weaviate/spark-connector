package io.weaviate.spark

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

case class WeaviateWriteBuilder(weaviateOptions: WeaviateOptions, schema: StructType) extends WriteBuilder with Serializable {
  override def build(): Write = WeaviateWrite(weaviateOptions, schema)
}
