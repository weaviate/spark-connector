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
    // TODO add support for all types such as DateType etc
    dataType match {
      case StringType => record.getString(index)
      case IntegerType => Int.box(record.getInt(index))
      case FloatType => Float.box(record.getFloat(index))
      case ArrayType(FloatType, true) => record.getArray(index)
      case ArrayType(IntegerType, true) => record.getArray(index)
      case DateType =>
        // Weaviate requires an RFC3339 formatted string and Spark stores a long that
        // contains the the days since EPOCH for DateType
        val daysSinceEpoch = record.getLong(index)
        java.time.LocalDate.ofEpochDay(daysSinceEpoch).toString + "T00:00:00Z"
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
