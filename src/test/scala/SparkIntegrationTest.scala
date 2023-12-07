package io.weaviate.spark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, Encoders}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import io.weaviate.client.v1.schema.model.Property
import org.apache.spark.SparkException

import scala.jdk.CollectionConverters._
import scala.reflect.io.Directory


class SparkIntegrationTest
  extends AnyFunSuite
    with SparkSessionTestWrapper
    with BeforeAndAfter {

  val client = WeaviateDocker.client

  before {
    val exit_code = WeaviateDocker.start()
    assert(exit_code == 0)
  }

  after {
    WeaviateDocker.stop()
  }

  test("Article with strings and int") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(Article("Sam", "Sam and Sam", 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    WeaviateDocker.deleteClass()
  }

  test("Article with empty strings and int") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(Article("", "Sam and Sam", 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(results.getResult.size == 1)
    assert(props.get("title") == "")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    WeaviateDocker.deleteClass()
  }

  test("Article with nulls") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(Article("Sam", null, 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(results.getResult.size == 1)
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "")
    assert(props.get("wordCount") == 3)
    WeaviateDocker.deleteClass()
  }

  test("Article with strings and int Streaming Write") {
    WeaviateDocker.createClass(
      Property.builder()
        .dataType(List[String]("string[]").asJava)
        .name("keywords")
        .build(),
      Property.builder()
        .dataType(List[String]("text").asJava)
        .name("customId")
        .build(),
      Property.builder()
        .dataType(List[String]("boolean").asJava)
        .name("myBool")
        .build(),
    )
    import spark.implicits._
    implicit val articleEncoder: Encoder[ArticleWithAll] = Encoders.product[ArticleWithAll]
    val inputStream: MemoryStream[ArticleWithAll] = new MemoryStream[ArticleWithAll](1, spark.sqlContext, Some(1))
    val inputStreamDF = inputStream.toDF

    val articles = List.fill(20)(ArticleWithAll("Sam", "Sam and Sam", 3, null, "not-used", true))

    val path = java.nio.file.Files.createTempDirectory("weaviate-spark-connector-streaming-test")
    val streamingWrite = inputStreamDF.writeStream
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("batchSize", 6)
      .option("className", "Article")
      .option("checkpointLocation", path.toAbsolutePath.toString)
      .outputMode("append")
      .start()

    inputStream.addData(articles)
    streamingWrite.processAllAvailable()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 20)
    WeaviateDocker.deleteClass()
    val dir = new Directory(path.toFile)
    dir.deleteRecursively()
  }

  test("Article with all datatypes") {
    import spark.implicits._
    case class TestCase(name: String, weaviateDataType: String, df: DataFrame, expected: Any)
    val cases = Seq(
      TestCase("byteTestCase", "int",
        Seq(ArticleWithByte("Sam", "Sam and Sam", 3, 1.toByte)).toDF, 1.0),
      TestCase("shortTestCase", "int",
        Seq(ArticleWithShort("Sam", "Sam and Sam", 3, 1.toShort)).toDF, 1.0),
      TestCase("longTestCase", "int",
        Seq(ArticleWithLong("Sam", "Sam and Sam", 3, 1.toLong)).toDF, 1.0),
      TestCase("floatTestCase", "number",
        Seq(ArticleWithFloat("Sam", "Sam and Sam", 3, 0.01f)).toDF, 0.01f),
      TestCase("doubleTestCase", "number",
        Seq(ArticleWithDouble("Sam", "Sam and Sam", 3, 0.01)).toDF, 0.01),
      TestCase("boolTestCase", "boolean",
        Seq(ArticleWithBoolean("Sam", "Sam and Sam", 3, true)).toDF, true),
    )

    for (c <- cases) {
      println(s"Running test case: ${c.name}")
      WeaviateDocker.createClass(
        Property.builder()
          .dataType(List[String](c.weaviateDataType).asJava)
          .name(c.name)
          .build()
      )

      c.df.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Article")
        .mode("append")
        .save()
      val results = client.data().objectsGetter()
        .withClassName("Article")
        .run()

      if (results.hasErrors) {
        println("Error getting Articles" + results.getError.getMessages)
      }

      assert(results.getResult.size == 1)
      val props = results.getResult.get(0).getProperties
      assert(props.get(c.name) == c.expected)
      WeaviateDocker.deleteClass()
    }
  }

  test("Article with strings and date") {
    WeaviateDocker.createClass(Property.builder()
      .dataType(List[String]("date").asJava)
      .name("date")
      .build()
    )
    import spark.implicits._
    val javaDate = java.sql.Date.valueOf("2022-11-18")
    val articles = Seq(ArticleWithDate("Sam", "Sam and Sam", 3, javaDate)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.size == 1)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(props.get("date") == "2022-11-18T00:00:00Z")
    WeaviateDocker.deleteClass()
  }

  test("Article with string array") {
    WeaviateDocker.createClass(Property.builder()
      .dataType(List[String]("string[]").asJava)
      .name("keywords")
      .build()
    )
    import spark.implicits._
    val articles = Seq(ArticleWithStringArray("Sam", "Sam and Sam", 3, Array("yo", "hey"))).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.size == 1)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(props.get("keywords") == List("yo", "hey").asJava)
    WeaviateDocker.deleteClass()
  }

  test("Article with empty string array") {
    WeaviateDocker.createClass(Property.builder()
      .dataType(List[String]("string[]").asJava)
      .name("keywords")
      .build()
    )
    import spark.implicits._
    val articles = Seq(ArticleWithStringArray("Sam", "Sam and Sam", 3, Array())).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.size == 1)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(props.get("keywords") == List().asJava)
    WeaviateDocker.deleteClass()
  }

  test("Article with double array") {
    WeaviateDocker.createClass(Property.builder()
      .dataType(List[String]("number[]").asJava)
      .name("doubles")
      .build()
    )
    import spark.implicits._
    val articles = Seq(ArticleWithDoubleArray("Sam", "Sam and Sam", 3, Array(1.0, 2.0, 3.0))).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.size == 1)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(props.get("doubles") == List(1.0, 2.0, 3.0).asJava)
    WeaviateDocker.deleteClass()
  }

  test("Article with int array") {
    WeaviateDocker.createClass(Property.builder()
      .dataType(List[String]("int[]").asJava)
      .name("ints")
      .build()
    )
    import spark.implicits._
    val articles = Seq(ArticleWithIntArray("Sam", "Sam and Sam", 3, Array(1, 2, 3))).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.size == 1)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(props.get("ints") == List(1.0, 2.0, 3.0).asJava) // TODO: why are these coming back as dubs?
    WeaviateDocker.deleteClass()
  }

  test("Test empty strings and large batch") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = (1 to 22).map(_ => Article("", "", 0)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("batchSize", 10)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }
    assert(results.getResult.size == 22)
    val props = (0 to 22 - 1).map(i => results.getResult.get(i).getProperties)
    results.getResult.forEach(obj => {
      assert(obj.getProperties.get("wordCount") == 0)
      assert(obj.getProperties.get("content") == "")
      assert(obj.getProperties.get("title") == "")
    })
    WeaviateDocker.deleteClass()
  }

  test("Test dataset size equal batch size") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = (1 to 10).map(_ => Article("", "", 0)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("batchSize", 10)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }
    assert(results.getResult.size == 10)
    val props = (0 to 10 - 1).map(i => results.getResult.get(i).getProperties)
    results.getResult.forEach(obj => {
      assert(obj.getProperties.get("wordCount") == 0)
      assert(obj.getProperties.get("content") == "")
      assert(obj.getProperties.get("title") == "")
    })
    WeaviateDocker.deleteClass()
  }

  test("Test dataset size being 1 less than batch size") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = (1 to 9).map(_ => Article("", "", 0)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("batchSize", 9)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }
    assert(results.getResult.size == 9)
    val props = (0 to 9 - 1).map(i => results.getResult.get(i).getProperties)
    results.getResult.forEach(obj => {
      assert(obj.getProperties.get("wordCount") == 0)
      assert(obj.getProperties.get("content") == "")
      assert(obj.getProperties.get("title") == "")
    })
    WeaviateDocker.deleteClass()
  }

  test("Test dataset size being 1 more than batch size") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = (1 to 11).map(_ => Article("", "", 0)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("batchSize", 11)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }
    assert(results.getResult.size == 11)
    val props = (0 to 11 - 1).map(i => results.getResult.get(i).getProperties)
    results.getResult.forEach(obj => {
      assert(obj.getProperties.get("wordCount") == 0)
      assert(obj.getProperties.get("content") == "")
      assert(obj.getProperties.get("title") == "")
    })
    WeaviateDocker.deleteClass()
  }

  test("Article different order") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(ArticleDifferentOrder("Sam and Sam", 3, "Sam")).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    WeaviateDocker.deleteClass()
  }

  test("Article with Spark provided vectors") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(ArticleWithVector("Sam", "Sam and Sam", 3, Array(0.01f, 0.02f))).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("vector", "vector")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .withVector()
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.get(0).getVector sameElements Array(0.01f, 0.02f))
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
    WeaviateDocker.deleteClass()
  }

  test("Article with custom IDs") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val id = java.util.UUID.randomUUID.toString
    val articles = Seq(ArticleWithID(id, "Sam", "Sam and Sam", 3)).toDF

    articles.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("className", "Article")
      .option("id", "idCol")
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName("Article")
      .run()

    if (results.hasErrors) {
      println("Error getting Articles" + results.getError.getMessages)
    }

    assert(results.getResult.get(0).getId == id)
    val props = results.getResult.get(0).getProperties
    assert(props.get("title") == "Sam")
    assert(props.get("content") == "Sam and Sam")
    assert(props.get("wordCount") == 3)
    assert(results.getResult.size == 1)
  }

  test("Article with invalid IDs") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val id = "invalid-uuid"
    val articles = Seq(ArticleWithID(id, "Sam", "Sam and Sam", 3)).toDF

    val exception = intercept[SparkException] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Article")
        .option("id", "idCol")
        .mode("append")
        .save()
    }
    assert(exception.getMessage.contains("""must be of type uuid: "invalid-uuid""""))
  }
  
  test("Article with partially invalid batch") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val id = java.util.UUID.randomUUID.toString
    val articles = Seq(
      ArticleWithID(id, "Sam", "Sam and Sam", 3),
      ArticleWithID("invalid-uuid", "Susie", "Big Cat", 2)
    ).toDF

    val exception = intercept[SparkException] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Article")
        .option("id", "idCol")
        .mode("append")
        .save()
    }
    assert(exception.getMessage.contains("""must be of type uuid: "invalid-uuid""""))
  }

  test("Article with extra columns") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(ArticleWithExtraCols("Sam", "Big Dog", "Sam and Sam", 3, 1900)).toDF

    assertThrows[AnalysisException] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("className", "Article")
        .mode("append")
        .save()
    }
  }

  test("Article with all datatypes using gRPC") {
    import spark.implicits._
    case class TestCase(name: String, weaviateDataType: String, df: DataFrame, expected: Any)
    val cases = Seq(
      TestCase("byteTestCase", "int",
        Seq(ArticleWithByte("Sam", "Sam and Sam", 3, 1.toByte)).toDF, 1.0),
      TestCase("shortTestCase", "int",
        Seq(ArticleWithShort("Sam", "Sam and Sam", 3, 1.toShort)).toDF, 1.0),
      TestCase("longTestCase", "int",
        Seq(ArticleWithLong("Sam", "Sam and Sam", 3, 1.toLong)).toDF, 1.0),
      TestCase("floatTestCase", "number",
        Seq(ArticleWithFloat("Sam", "Sam and Sam", 3, 0.01f)).toDF, 0.01f),
      TestCase("doubleTestCase", "number",
        Seq(ArticleWithDouble("Sam", "Sam and Sam", 3, 0.01)).toDF, 0.01),
      TestCase("boolTestCase", "boolean",
        Seq(ArticleWithBoolean("Sam", "Sam and Sam", 3, true)).toDF, true),
    )

    for (c <- cases) {
      println(s"Running test case: ${c.name}")
      WeaviateDocker.createClass(
        Property.builder()
          .dataType(List[String](c.weaviateDataType).asJava)
          .name(c.name)
          .build()
      )

      c.df.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("grpc:host", "localhost:50051")
        .option("className", "Article")
        .mode("append")
        .save()
      val results = client.data().objectsGetter()
        .withClassName("Article")
        .run()

      if (results.hasErrors) {
        println("Error getting Articles" + results.getError.getMessages)
      }

      assert(results.getResult.size == 1)
      val props = results.getResult.get(0).getProperties
      assert(props.get(c.name) == c.expected)
      WeaviateDocker.deleteClass()
    }
  }

  test("Object property with Books with gRPC") {
    val className = "Books"
    WeaviateDocker.createBooksClass()
    val df = spark.read.option("multiline", true).json("src/test/resources/books.json")

    df.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("grpc:host", "localhost:50051")
      .option("className", className)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName(className)
      .run()

    if (results.hasErrors) {
      println(s"Error getting ${className} ${results.getError.getMessages}")
    }

    assert(results.getResult.size == 3)
    results.getResult.forEach(r => {
      val props = r.getProperties
      assert(props.get("title") != null)
      assert(props.get("author") != null)
      assert(props.get("author").isInstanceOf[java.util.Map[String, Object]])
      val author = props.get("author").asInstanceOf[java.util.Map[String, Object]]
      assert(author.get("name") != null)
      assert(author.get("name").isInstanceOf[java.util.Map[String, Object]])
      val name = author.get("name").asInstanceOf[java.util.Map[String, Object]]
      assert(name.get("firstName") != null)
      assert(name.get("lastName") != null)
      if (name.get("firstName").asInstanceOf[String].equals("Stephen")) {
        assert(author.get("age") == null)
      } else {
        assert(author.get("age") != null)
      }
    })
    WeaviateDocker.deleteBooksClass()
  }

  test("Object Array property with Authors with gRPC") {
    val className = "Authors"
    WeaviateDocker.createAuthorsClass()
    val df = spark.read.option("multiline", true).json("src/test/resources/authors.json")

    df.write
      .format("io.weaviate.spark.Weaviate")
      .option("scheme", "http")
      .option("host", "localhost:8080")
      .option("grpc:host", "localhost:50051")
      .option("className", className)
      .mode("append")
      .save()

    val results = client.data().objectsGetter()
      .withClassName(className)
      .run()

    if (results.hasErrors) {
      println(s"Error getting ${className} ${results.getError.getMessages}")
    }

    assert(results.getResult.size == 4)
    results.getResult.forEach(r => {
      val props = r.getProperties
      assert(props.get("genre") != null)
      val genre = props.get("genre").asInstanceOf[String]
      if (genre.equals("to be defined") || genre.equals("empty")) {
        assert(props.get("authors") == null)
      } else {
        assert(props.get("authors") != null)
        assert(props.get("authors").isInstanceOf[java.util.List[java.util.Map[String, Object]]])
        val authors = props.get("authors").asInstanceOf[java.util.List[java.util.Map[String, Object]]]
        authors.forEach(author => {
          assert(author.get("firstName") != null)
          assert(author.get("lastName") != null)
          assert(author.get("isAlive") != null)
        })
      }
    })
    WeaviateDocker.deleteAuthorsClass()
  }

  test("Write with wrong Weaviate address") {
    WeaviateDocker.createClass()
    import spark.implicits._
    val articles = Seq(Article("Sam", "Sam and Sam", 3)).toDF

    val exception = intercept[WeaviateResultError] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:1234")
        .option("className", "Article")
        .mode("append")
        .save()
    }

    assert(exception.getMessage.contains("Connection refused"))
  }
}

class SparkIntegrationTestsOpenAI
  extends AnyFunSuite
    with SparkSessionTestWrapper
    with BeforeAndAfter {

  val client = WeaviateDocker.client

  before {
    val exit_code = WeaviateDocker.start(vectorizerModule = "text2vec-openai")
    assert(exit_code == 0)
  }

  after {
    WeaviateDocker.stop()
  }

  test("Set OpenAI API key with incorrect value") {
    WeaviateDocker.createClass()

    import spark.implicits._
    val articles = Seq(Article("Sam", "Big Dog", 3), Article("Susie", "Big Cat", 2)).toDF

    val exception = intercept[SparkException] {
      articles.write
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "localhost:8080")
        .option("header:X-OpenAI-Api-Key", "shouldntwork")
        .option("className", "Article")
        .mode("append")
        .save()
    }
    assert(exception.getMessage.contains("Incorrect API key provided: shouldntwork"))
  }
}