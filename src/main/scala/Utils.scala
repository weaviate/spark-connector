package io.weaviate.spark

import io.weaviate.client.v1.schema.model.Property
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.JavaConverters._
import java.util

object Utils {
  def weaviateToSparkDatatype(datatype: util.List[String], nestedProperties: util.List[Property.NestedProperty]): DataType = {
    datatype.get(0) match {
      case "string" => DataTypes.StringType
      case "string[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "int" => DataTypes.IntegerType
      case "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "boolean" => DataTypes.BooleanType
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "number" => DataTypes.DoubleType
      case "number[]" => DataTypes.createArrayType(DataTypes.DoubleType)
      case "date" => DataTypes.DateType
      case "date[]" => DataTypes.createArrayType(DataTypes.DateType)
      case "text" => DataTypes.StringType
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "object" => createStructType(nestedProperties)
      case "object[]" => DataTypes.createArrayType(createStructType(nestedProperties))
      case default => DataTypes.StringType
    }
  }

  private def createStructType(nestedProperties: util.List[Property.NestedProperty]): StructType = {
    val fields = nestedProperties.asScala.map(prop => {
      StructField(name = prop.getName, dataType = weaviateToSparkDatatype(prop.getDataType, prop.getNestedProperties))
    }).asJava

    DataTypes.createStructType(fields)
  }
}
