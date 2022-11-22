package io.weaviate.spark

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import technology.semi.weaviate.client.v1.schema.model.{Property, WeaviateClass}

import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}
import scala.sys.process._


class SparkIntegrationTest
  extends AnyFunSuite
    with SparkSessionTestWrapper
    with BeforeAndAfter {
  val options: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Map("scheme" -> "http", "host" -> "localhost:8080").asJava)
  val weaviateOptions: WeaviateOptions = new WeaviateOptions(options)
  val client = weaviateOptions.getClient()
  val logger = ProcessLogger(
    (o: String) => println("out " + o),
    (e: String) => println("err " + e))

  var retries = 5

  def createClass(additionalProperties: Property*): Unit = {
    val properties = Seq(
      Property.builder()
        .dataType(List[String]("string").asJava)
        .name("title")
        .build(),
      Property.builder()
        .dataType(List[String]("string").asJava)
        .name("content")
        .build(),
      Property.builder()
        .dataType(List[String]("int").asJava)
        .name("wordCount")
        .build(),
    ) ++ additionalProperties
    println(properties.map(_.getName))
    val clazz = WeaviateClass.builder.className("Article")
      .description("Article test class")
      .properties(properties.asJava).build

    val results = client.schema().classCreator().withClass(clazz).run
    if (results.hasErrors) {
      println("insert error" + results.getError.getMessages)
      if (retries > 1) {
        retries -= 1
        println("Retrying to create class in 1 seconds..")
        Thread.sleep(1000)
        createClass(additionalProperties: _*)
      }
    }
    println("Results: " + results.getResult)
    retries = 3
  }

  def deleteClass(): Unit = {
    val result = client.schema().classDeleter()
      .withClassName("Article")
      .run()
    if (result.hasErrors) println("Error deleting class Article " + result.getError.getMessages)
  }

  before {
    val docker_run =
      """docker run -d --name=weaviate-test-container-will-be-deleted
    -p 8080:8080
    -e QUERY_DEFAULTS_LIMIT=25
    -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true
    -e DEFAULT_VECTORIZER_MODULE=none
    -e CLUSTER_HOSTNAME=node1
    -e PERSISTENCE_DATA_PATH=./data
    semitechnologies/weaviate:1.16.1"""
    val exit_code = docker_run ! logger
    assert(exit_code == 0)
  }

  after {
    "docker stop weaviate-test-container-will-be-deleted" ! logger
    "docker rm weaviate-test-container-will-be-deleted" ! logger
  }


  test("Article with strings and int") {
    createClass()
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
    deleteClass()
  }

  test("Article with all number datatypes") {
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
    )

    for (c <- cases) {
      println(s"Running test case: ${c.name}")
      createClass(
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
      deleteClass()
    }
  }

  test("Article with strings and date") {
    createClass(Property.builder()
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
    deleteClass()
  }

  test("Test empty strings and large batch") {
    createClass()
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
    deleteClass()
  }

  test("Article different order") {
    createClass()
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
    deleteClass()
  }

  test("Article with Spark provided vectors") {
    createClass()
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
    deleteClass()
  }

  test("Article with custom IDs") {
    createClass()
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

  test("Article with extra columns") {
    createClass()
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
}

