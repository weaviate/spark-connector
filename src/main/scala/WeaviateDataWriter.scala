package io.weaviate.spark

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonSyntaxException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import io.weaviate.client.v1.data.model.WeaviateObject
import io.weaviate.client.v1.schema.model.WeaviateClass
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

import java.util.{Map => JavaMap}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._


case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType)
  extends DataWriter[InternalRow] with Serializable with Logging {
  var batch = mutable.Map[String, WeaviateObject]()
  private val weaviateClass = weaviateOptions.getWeaviateClass()

  override def write(record: InternalRow): Unit = {
    val weaviateObject = buildWeaviateObject(record, weaviateClass)
    batch += (weaviateObject.getId -> weaviateObject)

    if (batch.size >= weaviateOptions.batchSize) writeBatch()
  }

  def writeBatch(retries: Int = weaviateOptions.retries): Unit = {
    if (batch.size == 0) return

    val consistencyLevel = weaviateOptions.consistencyLevel
    val client = weaviateOptions.getClient()

    val results = if (consistencyLevel != "") {
      logInfo(s"Writing using consistency level: ${consistencyLevel}")
      client.batch().objectsBatcher().withObjects(batch.values.toList: _*).withConsistencyLevel(consistencyLevel).run()
    } else {
      client.batch().objectsBatcher().withObjects(batch.values.toList: _*).run()
    }

    val IDs = batch.keys.toList

    if (results.hasErrors || results.getResult == null) {
      if (retries == 0) {
        throw WeaviateResultError(s"error getting result and no more retries left." +
          s" Error from Weaviate: ${results.getError.getMessages}")
      }
      if (retries > 0) {
        logError(s"batch error: ${results.getError.getMessages}, will retry")
        logInfo(s"Retrying batch in ${weaviateOptions.retriesBackoff} seconds. Batch has following IDs: ${IDs}")
        Thread.sleep(weaviateOptions.retriesBackoff * 1000)
        writeBatch(retries - 1)
      }
    } else {
      val (objectsWithSuccess, objectsWithError) = results.getResult.partition(_.getResult.getErrors == null)
      if (objectsWithError.size > 0 && retries > 0) {
        val errors = objectsWithError.map(obj => s"${obj.getId}: ${obj.getResult.getErrors.toString}")
        val successIDs = objectsWithSuccess.map(_.getId).toList
        logWarning(s"Successfully imported ${successIDs}. " +
          s"Retrying objects with an error. Following objects in the batch upload had an error: ${errors.mkString("Array(", ", ", ")")}")
        batch = batch -- successIDs
        writeBatch(retries - 1)
      } else if (objectsWithError.size > 0) {
        val errorIds = objectsWithError.map(obj => obj.getId)
        val errorMessages = objectsWithError.map(obj => obj.getResult.getErrors.toString).distinct
        throw WeaviateResultError(s"Error writing to weaviate and no more retries left." +
          s" IDs with errors: ${errorIds.mkString("Array(", ", ", ")")}." +
          s" Error messages: ${errorMessages.mkString("Array(", ", ", ")")}")
      }
      else {
        logInfo(s"Writing batch successful. IDs of inserted objects: ${IDs}")
        batch.clear()
      }
    }
  }

  private[spark] def buildWeaviateObject(record: InternalRow, weaviateClass: WeaviateClass = null): WeaviateObject = {
    var builder = WeaviateObject.builder.className(weaviateOptions.className)
    if (weaviateOptions.tenant != null) {
      builder = builder.tenant(weaviateOptions.tenant)
    }
    val properties = mutable.Map[String, AnyRef]()
    val vectors = mutable.Map[String, Array[Float]]()
    val multiVectors = mutable.Map[String, Array[Array[Float]]]()
    schema.zipWithIndex.foreach(field =>
      field._1.name match {
        case weaviateOptions.vector => builder = builder.vector(record.getArray(field._2).toArray(FloatType))
        case key if weaviateOptions.vectors.contains(key) => vectors += (weaviateOptions.vectors(key) -> record.getArray(field._2).toArray(FloatType))
        case key if weaviateOptions.multiVectors.contains(key) => {
          val multiVectorArrayData = record.get(field._2, ArrayType(ArrayType(FloatType))) match {
            case array: ArrayData => array // Standard case: ArrayData
            case array: org.apache.spark.sql.catalyst.expressions.UnsafeArrayData => array // Standard case: ArrayData
            case rawArray: Array[_] if rawArray.forall(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData]) =>
              // Handle case where the field is a raw array of UnsafeArrayData
              new GenericArrayData(rawArray.asInstanceOf[Array[org.apache.spark.sql.catalyst.expressions.UnsafeArrayData]])
            case other => throw new IllegalArgumentException(s"Expected ArrayData, found ${other.getClass}")
          }
          val multiVector: Array[Array[Float]] = multiVectorArrayData.toArray[ArrayData](ArrayType(FloatType))
            .map(innerArrayData => innerArrayData.toFloatArray())

          multiVectors += (weaviateOptions.multiVectors(key) -> multiVector)
        }
        case weaviateOptions.id => builder = builder.id(record.getString(field._2))
        case _ => properties(field._1.name) = getPropertyValue(field._2, record, field._1.dataType, false, field._1.name, weaviateClass)
      }
    )
    if (weaviateOptions.id == null) {
      builder.id(java.util.UUID.randomUUID.toString)
    }

    if (vectors.nonEmpty) {
      builder.vectors(vectors.map { case (key, arr) => key -> arr.map(Float.box) }.asJava)
    }
    if (multiVectors.nonEmpty) {
      builder.multiVectors(multiVectors.map { case (key, multiVector) => key -> multiVector.map { vec => { vec.map(Float.box) }} }.toMap.asJava)
    }
    builder.properties(properties.asJava).build
  }

  def getPropertyValue(index: Int, record: InternalRow, dataType: DataType, parseObjectArrayItem: Boolean, propertyName: String, weaviateClass: WeaviateClass): AnyRef = {
    val valueFromField = getValueFromField(index, record, dataType, parseObjectArrayItem)
    if (weaviateClass != null) {
      var dt = ""
      weaviateClass.getProperties.forEach(p => {
        if (p.getName == propertyName) {
          // we are just looking for geoCoordinates or phoneNumber type
          dt = p.getDataType.get(0)
        }
      })
      if ((dt == "geoCoordinates" || dt == "phoneNumber") && valueFromField.isInstanceOf[String]) {
        return jsonToJavaMap(propertyName, valueFromField.toString).get
      }
    }
    valueFromField
  }

  def jsonToJavaMap(propertyName: String, jsonString: String): Option[JavaMap[String, Object]] = {
    try {
      val gson = new Gson()
      val mapType = new TypeToken[JavaMap[String, Object]]() {}.getType
      Some(gson.fromJson(jsonString, mapType))
    } catch {
      case e: JsonSyntaxException =>
        throw SparkDataTypeNotSupported(
          s"Error occured during parsing of $propertyName value: $jsonString")
    }
  }

  def getValueFromField(index: Int, record: InternalRow, dataType: DataType, parseObjectArrayItem: Boolean): AnyRef = {
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
      case ArrayType(DoubleType, true) | ArrayType(DoubleType, false) =>
        if (record.isNullAt(index)) Array[Double]() else record.getArray(index).toDoubleArray()
      case ArrayType(IntegerType, true) | ArrayType(IntegerType, false) =>
        if (record.isNullAt(index)) Array[Int]() else record.getArray(index).toIntArray()
      case ArrayType(StringType, true) | ArrayType(StringType, false) =>
        if (record.isNullAt(index)) {
          Array[String]()
        } else {
          record.getArray(index).toObjectArray(StringType).map(
            x => Option(x).filter(_ != null).map(_.toString).getOrElse(""))
        }
      case ArrayType(LongType, true) => throw new SparkDataTypeNotSupported(
        "Array of LongType is not supported. Convert to Spark Array of IntegerType instead")
      case DateType =>
        // Weaviate requires an RFC3339 formatted string and Spark stores an int that
        // contains the the days since EPOCH for DateType
        val daysSinceEpoch = record.getInt(index)
        java.time.LocalDate.ofEpochDay(daysSinceEpoch).toString + "T00:00:00Z"
      case StructType(_) =>
        val dt = dataType.asInstanceOf[StructType]
        val rec = if (parseObjectArrayItem) record else record.getStruct(index, dt.length)
        val objMap: mutable.Map[String, Object] = mutable.Map[String, Object]()
        dt.foreach(f => {
          if (!rec.isNullAt(dt.fieldIndex(f.name))) {
            objMap += (f.name -> getValueFromField(dt.fieldIndex(f.name), rec, f.dataType, parseObjectArrayItem))
          }
        })
        objMap.asJava
      case ArrayType(StructType(_), true) =>
        val dt = dataType.asInstanceOf[ArrayType]
        val fields = dt.elementType.asInstanceOf[StructType]
        val objList = ListBuffer[AnyRef]()
        if (!record.isNullAt(index)) {
          val arr = record.getArray(index).toObjectArray(fields)
          arr.foreach(o => {
            objList += getValueFromField(0, o.asInstanceOf[InternalRow], fields, parseObjectArrayItem = true)
          })
        }
        objList.asJava
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
