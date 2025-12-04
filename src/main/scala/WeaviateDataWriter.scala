package io.weaviate.spark

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonSyntaxException}
import io.weaviate.client6.v1.api.collections.{WeaviateObject, DataType => WeaviateDataType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import io.weaviate.client6.v1.api.collections.{CollectionConfig, Vectors}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

import java.util.{Map => JavaMap}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._


case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType)
  extends DataWriter[InternalRow] with Serializable with Logging {
  var batch = mutable.Map[String, WeaviateObject[JavaMap[String, Object]]]()
  private lazy val weaviateClass = weaviateOptions.getCollectionConfig()

  override def write(record: InternalRow): Unit = {
    val weaviateObject = buildWeaviateObject(record, weaviateClass)
    batch += (weaviateObject.uuid -> weaviateObject)

    if (batch.size >= weaviateOptions.batchSize) writeBatch()
  }

  def writeBatch(retries: Int = weaviateOptions.retries): Unit = {
    if (batch.isEmpty) return

    val client = weaviateOptions.getClient()

    val collection = client.collections
      .use(weaviateOptions.className)
      .withTenant(weaviateOptions.tenant)
      .withConsistencyLevel(weaviateOptions.consistencyLevel)

    val results = collection.data.insertMany(batch.values.toList.asJava)

    val IDs = batch.keys.toList

    if (results.errors() != null && !results.errors().isEmpty) {
      if (retries == 0) {
        throw WeaviateResultError(s"error getting result and no more retries left." +
          s" Error from Weaviate: ${results.errors().asScala.mkString(",")}")
      }
      if (retries > 0) {
        logError(s"batch error: ${results.errors().asScala.mkString(",")}, will retry")
        logInfo(s"Retrying batch in ${weaviateOptions.retriesBackoff} seconds. Batch has following IDs: ${IDs}")
        Thread.sleep(weaviateOptions.retriesBackoff * 1000)
        writeBatch(retries - 1)
      }
    } else {
      val (objectsWithSuccess, objectsWithError) = results.responses().asScala.partition(_.error() == null)
      if (objectsWithError.nonEmpty && retries > 0) {
        val errors = objectsWithError.map(obj => s"${obj.uuid()}: ${obj.error()}")
        val successIDs = objectsWithSuccess.map(_.uuid()).toList
        logWarning(s"Successfully imported ${successIDs}. " +
          s"Retrying objects with an error. Following objects in the batch upload had an error: ${errors.mkString("Array(", ", ", ")")}")
        batch = batch -- successIDs
        writeBatch(retries - 1)
      } else if (objectsWithError.nonEmpty) {
        val errorIds = objectsWithError.map(obj => obj.uuid())
        val errorMessages = objectsWithError.map(obj => obj.error()).distinct
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

  private[spark] def buildWeaviateObject(record: InternalRow, collectionConfig: CollectionConfig = null): WeaviateObject[java.util.Map[String, Object]] = {
    val builder: WeaviateObject.Builder[java.util.Map[String, Object]] = new WeaviateObject.Builder()

    val properties = mutable.Map[String, AnyRef]()
    var vector: Array[Float] = null
    val vectors = mutable.Map[String, Array[Float]]()
    val multiVectors = mutable.Map[String, Array[Array[Float]]]()
    schema.zipWithIndex.foreach(field =>
      field._1.name match {
        case weaviateOptions.vector => vector = record.getArray(field._2).toArray(FloatType)
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
        case weaviateOptions.id => builder.uuid(record.getString(field._2))
        case _ => properties(field._1.name) = getPropertyValue(field._2, record, field._1.dataType, false, field._1.name, collectionConfig)
      }
    )

    if (weaviateOptions.id == null) {
      builder.uuid(java.util.UUID.randomUUID.toString)
    }

    val allVectors = ListBuffer.empty[Vectors]
    if (vector != null) {
      allVectors += Vectors.of(vector)
    }
    if (vectors.nonEmpty) {
      allVectors ++= vectors.map { case (key, arr) => Vectors.of(key, arr) }
    }
    if (multiVectors.nonEmpty) {
      allVectors ++= multiVectors.map { case (key, multiVector) => Vectors.of(key, multiVector) }
    }

    builder.tenant(weaviateOptions.tenant).properties(properties.asJava).vectors(allVectors.toSeq : _*).build()
  }

  def getPropertyValue(index: Int, record: InternalRow, dataType: DataType, parseObjectArrayItem: Boolean, propertyName: String, collectionConfig: CollectionConfig): AnyRef = {
    val valueFromField = getValueFromField(index, record, dataType, parseObjectArrayItem)
    if (collectionConfig != null) {
      var dt = ""
      collectionConfig.properties().forEach(p => {
        if (p.propertyName() == propertyName) {
          // we are just looking for geoCoordinates or phoneNumber type
          dt = p.dataTypes().get(0)
        }
      })
      if ((dt == WeaviateDataType.GEO_COORDINATES || dt == WeaviateDataType.PHONE_NUMBER) && valueFromField.isInstanceOf[String]) {
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
      case default => throw SparkDataTypeNotSupported(s"DataType ${default} is not supported by Weaviate")
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
    // TODO rollback previously written batch results if issue occurred
    logError("Aborted data write")
  }
}
