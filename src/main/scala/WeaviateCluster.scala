package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._
import java.util

case class WeaviateCluster(weaviateOptions: WeaviateOptions, schema: StructType) extends SupportsWrite {
  override def name(): String = weaviateOptions.className

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    WeaviateWriteBuilder(weaviateOptions, schema)
  }
  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE
  ).asJava
}
