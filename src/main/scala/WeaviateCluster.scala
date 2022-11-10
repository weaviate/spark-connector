package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.util

case class WeaviateCluster() extends SupportsWrite {
  override def name(): String = ???
  override def schema(): StructType = ???
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???
  override def capabilities(): util.Set[TableCapability] = ???
}
