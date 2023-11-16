package io.weaviate.spark

import io.weaviate.client.v1.schema.model.{DataType, Property}
import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.JavaConverters._

class UtilsTest extends AnyFunSuite {
  test("Test Weaviate Datatype to Spark Datatype conversion") {
    assert(Utils.weaviateToSparkDatatype(List("string").asJava, null) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("string[]").asJava, null) == DataTypes.createArrayType(DataTypes.StringType))
    assert(Utils.weaviateToSparkDatatype(List("int").asJava, null) == DataTypes.IntegerType)
    assert(Utils.weaviateToSparkDatatype(List("int[]").asJava, null) == DataTypes.createArrayType(DataTypes.IntegerType))
    assert(Utils.weaviateToSparkDatatype(List("boolean").asJava, null) == DataTypes.BooleanType)
    assert(Utils.weaviateToSparkDatatype(List("boolean[]").asJava, null) == DataTypes.createArrayType(DataTypes.BooleanType))
    assert(Utils.weaviateToSparkDatatype(List("number").asJava, null) == DataTypes.DoubleType)
    assert(Utils.weaviateToSparkDatatype(List("number[]").asJava, null) == DataTypes.createArrayType(DataTypes.DoubleType))
    assert(Utils.weaviateToSparkDatatype(List("date").asJava, null) == DataTypes.DateType)
    assert(Utils.weaviateToSparkDatatype(List("date[]").asJava, null) == DataTypes.createArrayType(DataTypes.DateType))
    assert(Utils.weaviateToSparkDatatype(List("text").asJava, null) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("text[]").asJava, null) == DataTypes.createArrayType(DataTypes.StringType))
    assert(Utils.weaviateToSparkDatatype(List("geoCoordinates").asJava, null) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("phoneNumber").asJava, null) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("blob").asJava, null) == DataTypes.StringType)
    assert(Utils.weaviateToSparkDatatype(List("MyClassReference").asJava, null) == DataTypes.StringType)
    val objectProperty = getObjectPropertyType(DataType.OBJECT)
    val structProperty = Utils.weaviateToSparkDatatype(objectProperty.getDataType, objectProperty.getNestedProperties)
    assert(structProperty != null)
    val objectArrayProperty = getObjectPropertyType(DataType.OBJECT_ARRAY)
    val arrayStructProperty = Utils.weaviateToSparkDatatype(objectArrayProperty.getDataType, objectArrayProperty.getNestedProperties)
    assert(arrayStructProperty != null)
  }

  private def getObjectPropertyType(dataType: String): Property = {
    Property.builder()
      .name("objectProperty")
      .dataType(util.Arrays.asList(dataType))
      .nestedProperties(util.Arrays.asList(
        Property.NestedProperty.builder()
          .name("nestedInt")
          .dataType(util.Arrays.asList(DataType.INT))
          .build(),
        Property.NestedProperty.builder()
          .name("nestedNumber")
          .dataType(util.Arrays.asList(DataType.NUMBER))
          .build(),
        Property.NestedProperty.builder()
          .name("nestedText")
          .dataType(util.Arrays.asList(DataType.TEXT))
          .build(),
        Property.NestedProperty.builder()
          .name("nestedObjects")
          .dataType(util.Arrays.asList(DataType.OBJECT_ARRAY))
          .nestedProperties(util.Arrays.asList(
            Property.NestedProperty.builder()
              .name("nestedBoolLvl2")
              .dataType(util.Arrays.asList(DataType.BOOLEAN))
              .build(),
            Property.NestedProperty.builder()
              .name("nestedDateLvl2")
              .dataType(util.Arrays.asList(DataType.DATE))
              .build(),
            Property.NestedProperty.builder()
              .name("nestedNumbersLvl2")
              .dataType(util.Arrays.asList(DataType.NUMBER_ARRAY))
              .build(),
            Property.NestedProperty.builder()
              .name("moreNested")
              .dataType(util.Arrays.asList(DataType.OBJECT))
              .nestedProperties(util.Arrays.asList(
                Property.NestedProperty.builder()
                  .name("a")
                  .dataType(util.Arrays.asList(DataType.TEXT))
                  .build(),
                Property.NestedProperty.builder()
                  .name("b")
                  .dataType(util.Arrays.asList(DataType.NUMBER))
                  .build()
              ))
              .build()
          ))
          .build()
      ))
      .build()
  }
}
