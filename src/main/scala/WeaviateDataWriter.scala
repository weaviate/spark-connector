package io.weaviate.spark


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType, IntegerType, StringType, StructType}

import java.util
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter(weaviateOptions: WeaviateOptions, structType: StructType) extends DataWriter[InternalRow] with Serializable {

  override def write(record: InternalRow): Unit = {
    println("connecting to weaviate")
    val client = weaviateOptions.getClient()

    val properties = getPropertiesFromRecord(record)
    val results = client
      .data()
      .creator()
      .withProperties(properties)
      .withClassName(weaviateOptions.className)
      .run()
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages)
    }
    println("Results: " + results.toString)
  }

  def getValueFromField(index: Int, record: InternalRow, dataType: DataType): AnyRef = {
    dataType match {
      case StringType => record.getString(index)
      case IntegerType => Int.box(record.getInt(index))
      case FloatType => Float.box(record.getFloat(index))
      case ArrayType(FloatType, true) => record.getArray(index)
      case ArrayType(IntegerType, true) => record.getArray(index)
    }
  }

  private def getPropertiesFromRecord(record: InternalRow): util.Map[String, AnyRef] = {
    val properties = scala.collection.mutable.Map[String, AnyRef]()
    structType.zipWithIndex.foreach(f =>
      properties(f._1.name) = getValueFromField(f._2, record, f._1.dataType))

    properties.asJava
  }

  override def close(): Unit = {
    println("closed")
  }
  override def commit(): WriterCommitMessage = WeaviateCommitMessage("yo")
  override def abort(): Unit = {
    println("aborted")
  }
}
