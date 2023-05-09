package io.weaviate.spark

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DataTypes, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class TestWeaviateDataWriter extends AnyFunSuite {
  test("Test Build Weaviate Object without supplied ID") {
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
    assert(weaviateObject.getId != null)
  }

  test("Test Build Weaviate Object with supplied ID") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map(
        "scheme" -> "http", "host" -> "localhost",
        "className" -> "Article", "id" -> "id").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val uuid = java.util.UUID.randomUUID.toString
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), sam, sam, 5))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getId == uuid)
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

  test("Test Build Weaviate Object with Unsupported Data types") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "Article").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    case class TestCase(name: String, dataType: DataType, value: Any)
    val cases = Seq(
      TestCase("byte", DataTypes.ByteType, 1.toByte),
      TestCase("short", DataTypes.ShortType, 1.toByte),
      TestCase("long", DataTypes.LongType, 1.toByte),
      TestCase("float", DataTypes.FloatType, 0.01f),
      TestCase("map", DataTypes.createMapType(StringType, StringType, false), "test"),
    )

    for (c <- cases) {
      val structFields = Array[StructField](
        StructField(c.name, c.dataType, true, Metadata.empty),
      )
      val schema = StructType(structFields)
      val dw = WeaviateDataWriter(weaviateOptions, schema)
      val row = new GenericInternalRow(Array[Any](c.value))
      assertThrows[SparkDataTypeNotSupported] {
        dw.buildWeaviateObject(row)
      }
    }
  }

  test("Test write batch and with unresponsive Weaviate server") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "Article").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val uuid = java.util.UUID.randomUUID.toString
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), sam, sam, 5))
    val weaviateObject = dw.buildWeaviateObject(row)
    dw.batch(uuid) = weaviateObject
    dw.writeBatch(retries = 0)
  }
}
