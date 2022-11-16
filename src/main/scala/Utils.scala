package io.weaviate.spark

import org.apache.spark.sql.types.{DataType, DataTypes}

import java.util

object Utils {
  def weaviateToSparkDatatype(datatype: util.List[String]): DataType = {
    datatype.get(0) match {
      case "string[]" => DataTypes.createArrayType(DataTypes.StringType)
      case "int" => DataTypes.IntegerType
      case "int[]" => DataTypes.createArrayType(DataTypes.IntegerType)
      case "boolean" => DataTypes.BooleanType
      case "boolean[]" => DataTypes.createArrayType(DataTypes.BooleanType)
      case "number" => DataTypes.FloatType
      case "number[]" => DataTypes.createArrayType(DataTypes.FloatType)
      case "date" => DataTypes.DateType
      case "date[]" => DataTypes.createArrayType(DataTypes.DateType)
      case "text[]" => DataTypes.createArrayType(DataTypes.StringType)
      case default => DataTypes.StringType
    }
  }
}
