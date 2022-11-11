package io.weaviate.spark


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import technology.semi.weaviate.client.{Config, WeaviateClient}

import java.util
import scala.jdk.CollectionConverters._

case class WeaviateCommitMessage(msg: String) extends WriterCommitMessage

case class WeaviateDataWriter() extends DataWriter[InternalRow] with Serializable {
  val schema = Seq("title", "content")
  override def write(record: InternalRow): Unit = {
    println("connecting to weaviate")
    val config = new Config("http", "localhost:8080")
    val client = new WeaviateClient(config)


    val properties = getPropertiesFromRecord(record)
    val results = client
      .data()
      .creator()
      .withProperties(properties)
      .run()
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages.asScala.foreach(_.getMessage))
    }
    println("Results: " + results.toString)
  }

  private def getPropertiesFromRecord(record: InternalRow): util.Map[String, AnyRef] = {
    val properties = scala.collection.mutable.Map[String, AnyRef]()
    schema.zipWithIndex.foreach(itemWithIndex => properties(itemWithIndex._1) = record.getString(itemWithIndex._2))

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
