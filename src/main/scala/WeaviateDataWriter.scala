package io.weaviate.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import technology.semi.weaviate.client.v1.data.model.WeaviateObject

import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType)
  extends DataWriter[InternalRow] with Serializable with Logging {
  var batch = new mutable.ListBuffer[WeaviateObject]

  override def write(record: InternalRow): Unit = {
    batch += buildWeaviateObject(record)

    if (batch.size >= weaviateOptions.batchSize) writeBatch()
  }

  def writeBatch(): Unit = {
    val client = weaviateOptions.getClient()
    val results = client.batch().objectsBatcher().withObjects(batch.toList: _*).run()

    if (results.hasErrors) {
      logError("batch error" + results.getError.getMessages)
    }
    logInfo("Writing batch successful. IDs of inserted objects: " + results.getResult.map(_.getId).toList)
    batch.clear()
  }

  private[spark] def buildWeaviateObject(record: InternalRow): WeaviateObject = {
    var builder = WeaviateObject.builder.className(weaviateOptions.className)
    val properties = mutable.Map[String, AnyRef]()
    schema.zipWithIndex.foreach(field =>
      field._1.name match {
        case weaviateOptions.vector => builder = builder.vector(record.getArray(field._2).toArray(FloatType))
        case weaviateOptions.id => builder = builder.id(record.getString(field._2))
        case _ => properties(field._1.name) = getValueFromField(field._2, record, field._1.dataType)
      }
    )
    builder.properties(properties.asJava).build
  }

  def getValueFromField(index: Int, record: InternalRow, dataType: DataType): AnyRef = {
    dataType match {
      case StringType => record.getString(index)

      case ByteType => throw new SparkDataTypeNotSupported(
        "ByteType is not supported. Convert to Spark IntegerType instead")
      case ShortType => throw new SparkDataTypeNotSupported(
        "ShortType is not supported. Convert to Spark IntegerType instead")
      case IntegerType => Int.box(record.getInt(index))
      case LongType => throw new SparkDataTypeNotSupported(
        "LongType is not supported. Convert to Spark IntegerType instead")
      // FloatType is a 4 byte data structure however in Weaviate float64 is using
      // 8 bytes. So the 2 are not compatible and DoubleType (8 bytes) must be used.
      // inferSchema will always return DoubleType when it reads the Schema from Weaviate
      case FloatType => throw new SparkDataTypeNotSupported(
        "FloatType is not supported. Convert to Spark DoubleType instead")
      case DoubleType => Double.box(record.getDouble(index))
      case ArrayType(FloatType, true) => throw new SparkDataTypeNotSupported(
        "Array of FloatType is not supported. Convert to Spark Array of DoubleType instead")
      case ArrayType(DoubleType, true) => record.getArray(index)
      case ArrayType(IntegerType, true) => record.getArray(index)
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
