package io.weaviate.spark

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}

case class WeaviateWriteBuilder() extends WriteBuilder with Serializable {
  override def build(): Write = WeaviateWrite()
}
