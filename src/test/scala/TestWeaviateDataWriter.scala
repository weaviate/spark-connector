package io.weaviate.spark

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataType, DataTypes, Metadata, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class TestWeaviateDataWriter extends AnyFunSuite {
  test("Test Build Weaviate Object with Tenant") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost", "className" -> "Article", "tenant" -> "TenantA").asJava)
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
    assert(weaviateObject.getTenant == "TenantA")
  }

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
    assert(weaviateObject.getTenant == null)
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
    assert(weaviateObject.getTenant == null)
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
    val date = DateTimeUtils.fromJavaDate(javaDate)
    val row = new GenericInternalRow(Array[Any](sam, sam, 5, date))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getProperties.get("date") == "2022-11-18T00:00:00Z")
    assert(weaviateObject.getTenant == null)
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
    assertThrows[WeaviateResultError] {
      dw.writeBatch(retries = 0)
    }
  }

  test("Test Build Weaviate Object with vector") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map(
        "scheme" -> "http", "host" -> "localhost",
        "className" -> "Article", "id" -> "id",
        "vector" -> "embedding").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty),
      StructField("embedding", DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val uuid = java.util.UUID.randomUUID.toString
    val embedding = Array(0.111f, 0.222f)
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), sam, sam, 5, UnsafeArrayData.fromPrimitiveArray(embedding)))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getId == uuid)
    assert(weaviateObject.getVector != null)
    assert(weaviateObject.getVector.sameElements(embedding))
    assert(weaviateObject.getTenant == null)
  }

  test("Test Build Weaviate Object with vectors") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map(
        "scheme" -> "http", "host" -> "localhost",
        "className" -> "Article", "id" -> "id",
        "vectors:v1" -> "embedding1", "vectors:v2" -> "embedding2").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty),
      StructField("embedding1", DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty),
      StructField("embedding2", DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val uuid = java.util.UUID.randomUUID.toString
    val embedding1 = Array(0.111f, 0.222f)
    val embedding2 = Array(0.1f, 0.2f)
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), sam, sam, 5,
      UnsafeArrayData.fromPrimitiveArray(embedding1), UnsafeArrayData.fromPrimitiveArray(embedding2)))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getId == uuid)
    assert(weaviateObject.getVector == null)
    assert(weaviateObject.getVectors != null && weaviateObject.getVectors.size() == 2)
    assert(weaviateObject.getVectors.get("v1").sameElements(embedding1))
    assert(weaviateObject.getVectors.get("v2").sameElements(embedding2))
    assert(weaviateObject.getTenant == null)
  }

  test("Test Build Weaviate Object with vectors and multi vectors") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map(
        "scheme" -> "http", "host" -> "localhost",
        "className" -> "Article", "id" -> "id",
        "vectors:v1" -> "embedding1", "multivectors:colbert" -> "multivec").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("content", DataTypes.StringType, true, Metadata.empty),
      StructField("wordCount", DataTypes.IntegerType, true, Metadata.empty),
      StructField("embedding1", DataTypes.createArrayType(DataTypes.FloatType), true, Metadata.empty),
      StructField("multivec", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.FloatType)), true, Metadata.empty)
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val sam = UTF8String.fromString("Sam")
    val uuid = java.util.UUID.randomUUID.toString
    val embedding1 = Array(0.111f, 0.222f)
    val colbert = Array(Array(0.1f, 0.2f),Array(0.3f, 0.4f))
    val colbertArrayData = colbert.map(innerArray => UnsafeArrayData.fromPrimitiveArray(innerArray))
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), sam, sam, 5,
      UnsafeArrayData.fromPrimitiveArray(embedding1), colbertArrayData))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getProperties.get("title") == "Sam")
    assert(weaviateObject.getProperties.get("content") == "Sam")
    assert(weaviateObject.getProperties.get("wordCount") == 5)
    assert(weaviateObject.getId == uuid)
    assert(weaviateObject.getVector == null)
    assert(weaviateObject.getVectors != null && weaviateObject.getVectors.size() == 1)
    assert(weaviateObject.getVectors.get("v1").sameElements(embedding1))
    assert(weaviateObject.getMultiVectors != null && weaviateObject.getMultiVectors.size() == 1)
    assert(weaviateObject.getMultiVectors.get("colbert").length == 2)
    assert(weaviateObject.getMultiVectors.get("colbert")(0).sameElements(colbert(0)))
    assert(weaviateObject.getMultiVectors.get("colbert")(1).sameElements(colbert(1)))
    assert(weaviateObject.getTenant == null)
  }

  test("Test Build Weaviate Object with geo coordinates") {
    val options: CaseInsensitiveStringMap =
      new CaseInsensitiveStringMap(Map(
        "scheme" -> "http", "host" -> "localhost",
        "className" -> "GeoClass", "id" -> "id").asJava)
    val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
    val structFields = Array[StructField](
      StructField("id", DataTypes.StringType, true, Metadata.empty),
      StructField("title", DataTypes.StringType, true, Metadata.empty),
      StructField("geo", DataTypes.createStructType(
        Array(
          DataTypes.createStructField("latitude", DataTypes.DoubleType, false),
          DataTypes.createStructField("longitude", DataTypes.DoubleType, false)
        )
      ), true, Metadata.empty),
    )
    val schema = StructType(structFields)
    val dw = WeaviateDataWriter(weaviateOptions, schema)
    val uuid = java.util.UUID.randomUUID.toString
    val title = UTF8String.fromString("title")
    val latitude = 52.3932696
    val longitude = 4.8374263
    val geoLocationRow = new GenericInternalRow(Array[Any](latitude, longitude))
    val row = new GenericInternalRow(Array[Any](UTF8String.fromString(uuid), title, geoLocationRow))
    val weaviateObject = dw.buildWeaviateObject(row)

    assert(weaviateObject.getId == uuid)
    assert(weaviateObject.getProperties.get("title") == "title")
    assert(weaviateObject.getProperties.get("geo") != null)
    assert(weaviateObject.getTenant == null)
  }
}
