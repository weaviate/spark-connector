package io.weaviate.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import technology.semi.weaviate.client.v1.data.model.WeaviateObject

import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType)
  extends DataWriter[InternalRow] with Serializable with Logging {
  var batch = mutable.Map[String, WeaviateObject]()

  override def write(row: InternalRow): Unit = {
    val record = row.copy()
    if (record.numFields != schema.length) {
      throw WeaviateSparkNumberOfFieldsException(
        s"The record being written had ${record.numFields} fields, however there is only a schema" +
          s" defined for ${schema.length}. Schema: ${schema}")
    }
    val weaviateObject = buildWeaviateObject(record)
    batch += (weaviateObject.getId -> weaviateObject)

    if (batch.size >= weaviateOptions.batchSize) writeBatch()
  }

  def writeBatch(retries: Int = weaviateOptions.retries): Unit = {
    if (batch.size == 0) return
    val client = weaviateOptions.getClient()
    val results = client.batch().objectsBatcher().withObjects(batch.values.toList: _*).run()
    val IDs = batch.keys.toList

    if (results.hasErrors) {
      logError(s"batch error: ${results.getError.getMessages}")
      if (retries > 0) {
        logInfo(s"Retrying batch in ${weaviateOptions.retriesBackoff} seconds. Batch has following IDs: ${IDs}")
        Thread.sleep(weaviateOptions.retriesBackoff * 1000)
        writeBatch(retries - 1)
      }
    }

    val (objectsWithSuccess, objectsWithError) = results.getResult.partition(_.getResult.getErrors == null)
    if (objectsWithError.size > 0 && retries > 0) {
      val errors = objectsWithError.map(obj => s"${obj.getId}: ${obj.getResult.getErrors.toString}")
      val successIDs = objectsWithSuccess.map(_.getId).toList
      logWarning(s"Successfully imported ${successIDs}. " +
        s"Retrying objects with an error. Following objects in the batch upload had an error: ${errors.mkString("Array(", ", ", ")")}")
      batch = batch -- successIDs
      writeBatch(retries - 1)
    } else {
      logInfo(s"Writing batch successful. IDs of inserted objects: ${IDs}")
      batch.clear()
    }
  }

  private[spark] def buildWeaviateObject(record: InternalRow): WeaviateObject = {
    var builder = WeaviateObject.builder.className(weaviateOptions.className)
    logError(s"Schema: ${schema.length}, ${schema}")
    val row = record.toSeq(schema)

    val genericRow = new GenericRowWithSchema(record.toSeq(schema).toArray, schema)
    logError(s"Record ${record.numFields}: ${record}")
    logError(s"Record.toSeq ${genericRow.length}: ${genericRow}")
    val properties = mutable.Map[String, AnyRef]()
    (0 to schema.size - 1).map(i => {
      val field = schema(i)
      val value = row(i)
      field.name match {
        case weaviateOptions.vector => builder = builder.vector(record.getArray(i).toArray(FloatType))
        case weaviateOptions.id =>
          val uuidStr = convertFromSpark(value, field).toString
          logError(s"UUID (${field}): ${value}, ${uuidStr}, ${record.getUTF8String(i)}")
          builder = builder.id(java.util.UUID.fromString(uuidStr).toString)
        case _ =>
          logError(s"Handling field ${field.name}")
          properties(field.name) = convertFromSpark(value, field)
      }
    })
    //schema.zipWithIndex.foreach(field =>
    //  field._1.name match {
    //    case weaviateOptions.vector => builder = builder.vector(record.getArray(field._2).toArray(FloatType))
    //    case weaviateOptions.id =>
    //      val uuidStr = convertFromSpark(row(field._2), field._1).toString
    //      logError(s"UUID: ${uuidStr}, ${record.getString(field._2)}")
    //      builder = builder.id(java.util.UUID.fromString(uuidStr).toString)
    //    case _ =>
    //      logError(s"Handling field ${field._1.name}")
    //      properties(field._1.name) = convertFromSpark(row(field._2), field._1)
    //  }
    //)
    if (weaviateOptions.id == null) {
      builder.id(java.util.UUID.randomUUID.toString)
    }
    builder.properties(properties.asJava).build
  }

  private def extractStructType(dataType: DataType): StructType = dataType match {
    case arrayType: ArrayType => extractStructType(arrayType.elementType)
    case _ => throw new UnsupportedOperationException(s"$dataType not supported")
  }

  def convertFromSpark(value: Any, fieldType: StructField = null): AnyRef = value match {
    case int: Int if fieldType.dataType == DateType =>
      java.time.LocalDate.ofEpochDay(int).toString + "T00:00:00Z"
    case long: Long if fieldType.dataType == DateType =>
      java.time.LocalDate.ofEpochDay(long).toString + "T00:00:00Z"
    case string if fieldType.dataType == StringType =>
      if (string == null) {
        ""
      } else {
        logError("Before toString")
        string.toString
      }
    case unsafeRow: UnsafeRow =>
      logError("unsafeRow")
      val structType = extractStructType(fieldType.dataType)
      val row = new GenericRowWithSchema(unsafeRow.toSeq(structType).toArray, structType)
      convertFromSpark(row)
    case unsafeArray: UnsafeArrayData => {
      val sparkType = fieldType.dataType match {
        case arrayType: ArrayType => arrayType.elementType
        case _ => fieldType.dataType
      }
      logError(s"unsafe array ${sparkType}, ${fieldType.name}:${fieldType.dataType}")
      if (unsafeArray == null || unsafeArray.numElements() == 0) {
        logError("unsafe Array empty")
        Array[AnyRef]()
      } else {
        logError("unsafe Array not empty")
        unsafeArray.toSeq[AnyRef](sparkType)
          .map(elem => convertFromSpark(elem, StructField("", sparkType, true)))
          .asJava
      }
    }
    case string: UTF8String => string.toString
    case default =>
      logError(s"Field dataType: ${fieldType.dataType}")
      default.asInstanceOf[AnyRef]
  }

  def getValueFromField(index: Int, record: InternalRow, dataType: DataType): AnyRef = {
    dataType match {
      case StringType => if (record.isNullAt(index)) "" else record.getUTF8String(index).toString
      case BooleanType => if (record.isNullAt(index)) Boolean.box(false) else Boolean.box(record.getBoolean(index))
      case ByteType => throw new SparkDataTypeNotSupported(
        "ByteType is not supported. Convert to Spark IntegerType instead")
      case ShortType => throw new SparkDataTypeNotSupported(
        "ShortType is not supported. Convert to Spark IntegerType instead")
      case IntegerType => if (record.isNullAt(index)) Int.box(0) else Int.box(record.getInt(index))
      case LongType => throw new SparkDataTypeNotSupported(
        "LongType is not supported. Convert to Spark IntegerType instead")
      // FloatType is a 4 byte data structure however in Weaviate float64 is using
      // 8 bytes. So the 2 are not compatible and DoubleType (8 bytes) must be used.
      // inferSchema will always return DoubleType when it reads the Schema from Weaviate
      case FloatType => throw new SparkDataTypeNotSupported(
        "FloatType is not supported. Convert to Spark DoubleType instead")
      case DoubleType => if (record.isNullAt(index)) Double.box(0.0) else Double.box(record.getDouble(index))
      case ArrayType(FloatType, true) => throw new SparkDataTypeNotSupported(
        "Array of FloatType is not supported. Convert to Spark Array of DoubleType instead")
      case ArrayType(DoubleType, true) =>
        if (record.isNullAt(index)) Array[Double]() else record.getArray(index).toDoubleArray()
      case ArrayType(IntegerType, true) =>
        if (record.isNullAt(index)) Array[Int]() else record.getArray(index).toIntArray()
      case ArrayType(StringType, true) =>
        if (record.isNullAt(index)) {
          Array[String]()
        } else {
          record.getArray(index).toObjectArray(StringType).map(
            x => Option(x).filter(_ != null).map(_.toString).getOrElse(""))
        }
      case ArrayType(LongType, true) => throw new SparkDataTypeNotSupported(
        "Array of LongType is not supported. Convert to Spark Array of IntegerType instead")
      case DateType =>
        // Weaviate requires an RFC3339 formatted string and Spark stores a long that
        // contains the the days since EPOCH for DateType
        val daysSinceEpoch = record.getLong(index)
        java.time.LocalDate.ofEpochDay(daysSinceEpoch).toString + "T00:00:00Z"
      case default => throw new SparkDataTypeNotSupported(s"DataType ${default} is not supported by Weaviate")
    }
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
