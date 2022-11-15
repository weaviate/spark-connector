package io.weaviate.spark


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._
import technology.semi.weaviate.client.v1.data.model.WeaviateObject

import java.util
import scala.collection.JavaConverters.{mutableMapAsJavaMapConverter => _, _}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, schema: StructType) extends DataWriter[InternalRow] with Serializable {
  var batch = new ListBuffer[WeaviateObject]

  override def write(record: InternalRow): Unit = {
    val properties = getPropertiesFromRecord(record)
    var obj = WeaviateObject.builder.className(weaviateOptions.className)
    if (weaviateOptions.id != null) {
      obj = obj.id(properties.get(weaviateOptions.id).get.toString)
      properties.remove(weaviateOptions.id)
    }
    obj = obj.properties(properties.asJava)
    batch += obj.build()

    if (batch.size >= weaviateOptions.batchSize) writeBatch
  }

  def writeBatch(): Unit = {
    val client = weaviateOptions.getClient()
    val results = client.batch().objectsBatcher().withObjects(batch.toList: _*).run()

    if (results.hasErrors) {
      println("batch error" + results.getError.getMessages)
    }
    println("Writing batch successful. Results: " + results.getResult)
    batch.clear()
  }

  def getValueFromField(index: Int, record: InternalRow, dataType: DataType): AnyRef = {
    // TODO add support for all types such as DateType etc
    dataType match {
      case StringType => record.getString(index)
      case IntegerType => Int.box(record.getInt(index))
      case FloatType => Float.box(record.getFloat(index))
      case ArrayType(FloatType, true) => record.getArray(index)
      case ArrayType(IntegerType, true) => record.getArray(index)
    }
  }

  private def getPropertiesFromRecord(record: InternalRow): scala.collection.mutable.Map[String, AnyRef] = {
    val properties = scala.collection.mutable.Map[String, AnyRef]()
    schema.zipWithIndex.foreach(field =>
      properties(field._1.name) = getValueFromField(field._2, record, field._1.dataType)
    )
    properties
  }

  override def close(): Unit = {
    // TODO add logic for closing
    println("closed")
  }

  override def commit(): WriterCommitMessage = {
    writeBatch()
    WeaviateCommitMessage("yo")
  }

  override def abort(): Unit = {
    // TODO rollback previously written batch results if issue occured
    println("aborted")
  }
}
