package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import technology.semi.weaviate.client.Config
import technology.semi.weaviate.client.WeaviateClient

import java.util
import scala.jdk.CollectionConverters._

class Weaviate extends TableProvider with DataSourceRegister {
  override def shortName(): String = "weaviate"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = StructType(
    Array(StructField("title", StringType, nullable = true), StructField("content", StringType, nullable = true))
  )
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    WeaviateCluster()
}
