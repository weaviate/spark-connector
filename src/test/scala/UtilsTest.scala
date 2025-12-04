package io.weaviate.spark

import io.weaviate.client6.v1.api.collections.{DataType, Property}
import org.apache.spark
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

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
    val structProperty = Utils.weaviateToSparkDatatype(objectProperty.dataTypes(), objectProperty.nestedProperties())
    assertStructType(structProperty)
    val objectArrayProperty = getObjectPropertyType(DataType.OBJECT_ARRAY)
    val arrayStructProperty = Utils.weaviateToSparkDatatype(objectArrayProperty.dataTypes(), objectArrayProperty.nestedProperties())
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
    val nestedObjects = st.find(_.name == "nestedObjects").get
    // check nestedObjects fields
    val nestedNames = List("nestedDateLvl2", "nestedBoolLvl2", "nestedNumbersLvl2", "moreNested")
    val nestedObjectsFields = nestedObjects.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType].fields
    nestedObjectsFields.foreach(f => {
      assert(nestedNames.contains(f.name))
    })
    // check moreNested fields
    val moreNested = nestedObjectsFields.find(_.name == "moreNested").get
    val moreNestedNames = List("a", "b")
    val moreNestedFields = moreNested.dataType.asInstanceOf[StructType].fields
    moreNestedFields.foreach(f => {
      moreNestedNames.contains(f.name)
    })
  }

  private def getObjectPropertyType(dataType: String): Property = {
    val nestedObjects = new Property.Builder("nestedObjects", DataType.OBJECT_ARRAY)
      .nestedProperties(
        Property.bool("nestedBoolLvl2"),
        Property.date("nestedDateLvl2"),
        Property.numberArray("nestedNumbersLvl2"),
        Property.`object`("moreNested", moreNested => moreNested.nestedProperties(
          Property.text("a"),
          Property.number("b")
        ))
      ).build()

    new Property.Builder("objectProperty", dataType)
      .nestedProperties(
        Property.integer("nestedInt"),
        Property.number("nestedNumber"),
        Property.text("nestedText"),
        nestedObjects
      ).build()
  }
}
