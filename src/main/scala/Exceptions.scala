package io.weaviate.spark

case class SparkDataTypeNotSupported(s: String) extends Exception(s) {}

case class WeaviateResultError(s: String) extends Exception(s) {}

case class WeaviateClassNotFoundError(s: String) extends Exception(s) {}

case class WeaviateSparkNumberOfFieldsException(s: String) extends Exception(s) {}