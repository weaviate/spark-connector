package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.jdk.CollectionConverters._
import java.util

case class WeaviateCluster() extends SupportsWrite {
  override def name(): String = "myCluster"
  override def schema(): StructType = StructType(
    Array(StructField("title", StringType, nullable = false), StructField("content", StringType, nullable = false))
  )
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = WeaviateWriteBuilder()
  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE
  ).asJava

}
