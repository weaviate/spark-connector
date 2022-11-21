package io.weaviate.spark

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class TestWeaviateDataWriter extends AnyFunSuite {
  test("Test Build Weaviate Object") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "Article").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val row = new GenericInternalRow(Array[Any](sam, sam, 5))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getId == null)
  }

  test("Test Build Weaviate Object with DateString") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "Article").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty),
      StructField("date", DataTypes.DateType, true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val javaDate = java.sql.Date.valueOf("2022-11-18")
    val date = DateTimeUtils.fromJavaDate(javaDate).asInstanceOf[Long]
    val row = new GenericInternalRow(Array[Any](sam, sam, 5, date))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getProperties.get("date") == "2022-11-18T00:00:00Z")
  }
}
