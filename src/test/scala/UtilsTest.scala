package io.weaviate.spark

import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class UtilsTest extends AnyFunSuite {
  test("Test Weaviate Datatype to Spark Datatype conversion") {
    assert(Utils.weaviateToSparkDatatype(List("string").asJava) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("string[]").asJava) == DataTypes.createArrayType(DataTypes.StringType))
    assert(Utils.weaviateToSparkDatatype(List("int").asJava) == DataTypes.IntegerType)
    assert(Utils.weaviateToSparkDatatype(List("int[]").asJava) == DataTypes.createArrayType(DataTypes.IntegerType))
    assert(Utils.weaviateToSparkDatatype(List("boolean").asJava) == DataTypes.BooleanType)
    assert(Utils.weaviateToSparkDatatype(List("boolean[]").asJava) == DataTypes.createArrayType(DataTypes.BooleanType))
    assert(Utils.weaviateToSparkDatatype(List("number").asJava) == DataTypes.FloatType)
    assert(Utils.weaviateToSparkDatatype(List("number[]").asJava) == DataTypes.createArrayType(DataTypes.FloatType))
    assert(Utils.weaviateToSparkDatatype(List("date").asJava) == DataTypes.DateType)
    assert(Utils.weaviateToSparkDatatype(List("date[]").asJava) == DataTypes.createArrayType(DataTypes.DateType))
    assert(Utils.weaviateToSparkDatatype(List("text").asJava) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("text[]").asJava) == DataTypes.createArrayType(DataTypes.StringType))
    assert(Utils.weaviateToSparkDatatype(List("geoCoordinates").asJava) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("phoneNumber").asJava) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("blob").asJava) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("MyClassReference").asJava) == DataTypes.StringType)
  }
}
