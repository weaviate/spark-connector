package io.weaviate.spark

case class Article(title: String, content: String, wordCount: Int)

case class ArticleWithVector(title: String, content: String, wordCount: Int, vector: Array[Float])

case class ArticleWithEmbedding(title: String, content: String, wordCount: Int, embedding: Array[Float])

case class ArticleDifferentOrder(content: String, wordCount: Int, title: String)

case class ArticleWithID(idCol: String, title: String, content: String, wordCount: Int)

case class ArticleWithExtraCols(title: String, author: String, content: String, wordCount: Int, ifpaRank: Int)

case class ArticleWithDate(title: String, content: String, wordCount: Int, date: java.sql.Date)

case class ArticleWithByte(title: String, content: String, wordCount: Int, byteTestCase: Byte)

case class ArticleWithShort(title: String, content: String, wordCount: Int, shortTestCase: Short)

case class ArticleWithLong(title: String, content: String, wordCount: Int, longTestCase: Long)

case class ArticleWithDouble(title: String, content: String, wordCount: Int, doubleTestCase: Double)

case class ArticleWithFloat(title: String, content: String, wordCount: Int, floatTestCase: Float)

case class ArticleWithBoolean(title: String, content: String, wordCount: Int, boolTestCase: Boolean)

case class ArticleWithStringArray(title: String, content: String, wordCount: Int, keywords: Array[String])

case class ArticleWithDoubleArray(title: String, content: String, wordCount: Int, doubles: Array[Double])

case class ArticleWithIntArray(title: String, content: String, wordCount: Int, ints: Array[Int])

case class ArticleWithAll(title: String, content: String, wordCount: Int, keywords: Array[String],
                          customId: String, myBool: Boolean)

case class RegularVectorsWithVectors(title: String, embedding: Array[Float])

case class MultiVectorWithAllVectors(title: String, regularVector: Array[Float], multiVector: Array[Array[Float]])
