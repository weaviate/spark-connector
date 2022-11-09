package io.weaviate.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import technology.semi.weaviate.client.Config
import technology.semi.weaviate.client.WeaviateClient

import java.util
import scala.jdk.CollectionConverters._

class Weaviate extends TableProvider with DataSourceRegister {
  override def shortName(): String = "weaviate"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val config = new Config("http", "localhost:8080")
    val client = new WeaviateClient(config)
    val className = options.get("className")
    val result = client.schema.classGetter.withClassName(className).run
    val properties = result.getResult.getProperties.asScala
    // getDataType returns List<string> so need to think about how to convert to Spark datatype
    val structFields = properties.map(p => StructField(p.getName(), DataTypes.StringType, true, Metadata.empty))
    new StructType(structFields.toArray)
  }
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = ???
}
