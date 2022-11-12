package io.weaviate.spark

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}

case class WeaviateWriteBuilder(weaviateOptions: WeaviateOptions) extends WriteBuilder with Serializable {
  override def build(): Write = WeaviateWrite(weaviateOptions)
}
