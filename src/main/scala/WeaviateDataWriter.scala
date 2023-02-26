package io.weaviate.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import technology.semi.weaviate.client.v1.data.model.WeaviateObject

import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType)
  extends DataWriter[InternalRow] with Serializable with Logging {
  var batch = mutable.Map[String, WeaviateObject]()

  override def write(record: InternalRow): Unit = {
    if (record.numFields != schema.length) {
      throw WeaviateSparkNumberOfFieldsException(
        s"The record being written had ${record.numFields} fields, however there is only a schema" +
          s" defined for ${schema.length}. Schema: ${schema}")
    }
    val weaviateObject = buildWeaviateObject(record)
    batch += (weaviateObject.getId -> weaviateObject)
    if (batch.size >= weaviateOptions.batchSize) writeBatch()
  }

  def writeBatch(): Unit = {
    if (batch.size == 0) return
    val client = weaviateOptions.getClient()
    val results = client.batch().objectsBatcher().withObjects(batch.values.toList: _*).run()
    val IDs = batch.keys.toList
    logInfo(s"Batch was sent to Weaviate. IDs of objects in the batch: ${IDs}")
    if (results.hasErrors) {
      logError(s"The batch had errors: ${results.getError.getMessages}")
    }
    batch.clear()
  }

  private[spark] def buildWeaviateObject(record: InternalRow): WeaviateObject = {
    var builder = WeaviateObject.builder.className(weaviateOptions.className)
    val row = record.toSeq(schema)

    val properties = mutable.Map[String, AnyRef]()

    (0 to schema.size - 1).map(i => {
      val field = schema(i)
      val value = row(i)
      field.name match {
        case weaviateOptions.vector => builder = builder.vector(record.getArray(i).toArray(FloatType))
        case weaviateOptions.id =>
          val uuidStr = convertFromSpark(value, field).toString
          builder = builder.id(java.util.UUID.fromString(uuidStr).toString)
        case _ =>
          throwForUnSupportedTypes(field)
          properties(field.name) = convertFromSpark(value, field)
      }
    })
    if (weaviateOptions.id == null) {
      builder.id(java.util.UUID.randomUUID.toString)
    }
    builder.properties(properties.asJava).build
  }

  def throwForUnSupportedTypes(field: StructField): Unit = field.dataType match {
    case _: MapType => throw SparkDataTypeNotSupported(s"f")
    case ByteType | ShortType | LongType => throw new SparkDataTypeNotSupported(
      s"Field '${field.name} of type ${field.dataType.toString} is not supported. " +
        s"Convert to Spark IntegerType instead")
    case FloatType => throw new SparkDataTypeNotSupported(
      "FloatType is not supported. Convert to Spark DoubleType instead")
    case ArrayType(FloatType, true) => throw new SparkDataTypeNotSupported(
      "Array of FloatType is not supported. Convert to Spark Array of DoubleType instead")
    case ArrayType(LongType, true) => throw new SparkDataTypeNotSupported(
      "Array of LongType is not supported. Convert to Spark Array of IntegerType instead")
    case _ =>
  }


  private def extractStructType(dataType: DataType): StructType = dataType match {
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw SparkDataTypeNotSupported(s"$dataType not supported")
  }

  def convertFromSpark(value: Any, field: StructField = null): AnyRef = value match {
    case x: Long if field.dataType == DateType =>
      java.time.LocalDate.ofEpochDay(x).toString + "T00:00:00Z"
    case x: Int if field.dataType == DateType =>
      java.time.LocalDate.ofEpochDay(x).toString + "T00:00:00Z"
    case string if field.dataType == StringType =>
      if (string == null) {
        ""
      } else {
        string.toString
      }
    case unsafeRow: UnsafeRow =>
      val structType = extractStructType(field.dataType)
      val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
      convertFromSpark(row)
    case unsafeArray: UnsafeArrayData =>
      val sparkType = field.dataType match {
        case arrayType: ArrayType => arrayType.elementType
        case _ => field.dataType
      }
      if (unsafeArray == null || unsafeArray.numElements() == 0) {
        Array[AnyRef]()
      } else {
        unsafeArray.toSeq[AnyRef](sparkType)
          .map(elem => convertFromSpark(elem, StructField("", sparkType, true)))
          .asJava
      }
    case default =>
      default.asInstanceOf[AnyRef]
  }

  override def close(): Unit = {
    // TODO add logic for closing
    logInfo("closed")
  }

  override def commit(): WriterCommitMessage = {
    writeBatch()
    WeaviateCommitMessage("Weaviate data committed")
  }

  override def abort(): Unit = {
    // TODO rollback previously written batch results if issue occured
    logError("Aborted data write")
  }
}
