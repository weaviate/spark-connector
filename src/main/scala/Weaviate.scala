package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

case class Weaviate() extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = ???

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = ???
}
