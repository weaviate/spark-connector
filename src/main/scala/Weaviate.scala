package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

class Weaviate extends TableProvider with DataSourceRegister {
  override def shortName(): String = "weaviate"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val weaviateOptions = new WeaviateOptions(options)
    val client = weaviateOptions.getClient()
    val className = weaviateOptions.className
    val result = client.collections.getConfig(className)
    if (result.isEmpty) throw WeaviateClassNotFoundError(s"Collection ${className} was not found.")
    val properties = result
      .map(r => r.properties().asScala)
      .orElseThrow(() => WeaviateClassNotFoundError(s"Collection ${className} was not found."))
    val structFields = properties.map(p =>
      StructField(p.propertyName(), Utils.weaviateToSparkDatatype(p.dataTypes(), p.nestedProperties()), true, Metadata.empty))
    if (weaviateOptions.vector != null)
      structFields.append(StructField(weaviateOptions.vector, DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty))
    if (weaviateOptions.id != null)
      structFields.append(StructField(weaviateOptions.id, DataTypes.StringType, true, Metadata.empty))
    if (weaviateOptions.vectors != null)
      weaviateOptions.vectors.foreach { case (vectorColumnName, _) =>
        structFields.append(StructField(vectorColumnName, DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty))
      }
    if (weaviateOptions.multiVectors != null)
      weaviateOptions.multiVectors.foreach { case (vectorColumnName, _) =>
        structFields.append(StructField(vectorColumnName, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.FloatType)), true, Metadata.empty))
      }
    new StructType(structFields.toArray)
  }
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val weaviateOptions = new WeaviateOptions(new CaseInsensitiveStringMap(properties))
    WeaviateCluster(weaviateOptions, schema)
  }
}
