package io.weaviate.spark

import io.weaviate.client.v1.schema.model.{DataType, Property}
import org.apache.spark
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
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
    assertStructType(structProperty)
    val objectArrayProperty = getObjectPropertyType(DataType.OBJECT_ARRAY)
    val arrayStructProperty = Utils.weaviateToSparkDatatype(objectArrayProperty.getDataType, objectArrayProperty.getNestedProperties)
    assert(arrayStructProperty != null)
    assert(arrayStructProperty.isInstanceOf[ArrayType])
    assert(arrayStructProperty.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType])
    assertStructType(arrayStructProperty.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
  }

  private def assertStructType(dt: spark.sql.types.DataType): Unit = {
    assert(dt != null)
    assert(dt.isInstanceOf[StructType])
    val st = dt.asInstanceOf[StructType]
    // check objectProperty fields
    assert(st.fieldIndex("nestedInt") == 0)
    assert(st.fieldIndex("nestedNumber") == 1)
    assert(st.fieldIndex("nestedText") == 2)
    assert(st.fieldIndex("nestedObjects") == 3)
    val nestedObjects = st.toIterator.find(_.name == "nestedObjects").get
    // check nestedObjects fields
    val nestedNames = List("nestedDateLvl2", "nestedBoolLvl2", "nestedNumbersLvl2", "moreNested")
    val nestedObjectsFields = nestedObjects.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields
    nestedObjectsFields.foreach(f => {
      assert(nestedNames.contains(f.name))
    })
    // check moreNested fields
    val moreNested = nestedObjectsFields.toIterator.find(_.name == "moreNested").get
    val moreNestedNames = List("a", "b")
    val moreNestedFields = moreNested.dataType.asInstanceOf[StructType].fields
    moreNestedFields.foreach(f => {
      moreNestedNames.contains(f.name)
    })
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
