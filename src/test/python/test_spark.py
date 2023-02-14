import time
import logging
import os
from os import path
import json
import re
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pytest
import docker
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, BooleanType, IntegerType, \
    DateType
import weaviate
import pandas as pd
import py4j
from testcontainers.kafka import KafkaContainer
from kafka import KafkaProducer

from .movie_schema import movie_schema


def get_connector_version():
    version_file = open("version.sbt", "r")
    version_str = version_file.read()
    version_file.close()
    return re.search(r"version :=\ \"(.*)\"", version_str).group(1)


connector_version = os.environ.get("CONNECTOR_VERSION", get_connector_version())
scala_version = os.environ.get("SCALA_VERSION", "2.12")
weaviate_version = os.environ.get("WEAVIATE_VERSION", "1.17.2")
spark_connector_jar_path = os.environ.get(
    "CONNECTOR_JAR_PATH", f"target/scala-{scala_version}/spark-connector-assembly-{connector_version}.jar"
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("Weaviate Pyspark Tests")
        .master('local')
        .config("spark.jars", spark_connector_jar_path)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("INFO")
    return spark


@pytest.fixture
def weaviate_client():
    container_name = "weaviate-python-integration-tests"
    client = docker.from_env()
    client.containers.run(
        f"semitechnologies/weaviate:{weaviate_version}",
        detach=True,
        name=container_name,
        ports={"8080/tcp": "8080"},
        environment={"QUERY_DEFAULTS_LIMIT": 25,
                     "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true",
                     "DEFAULT_VECTORIZER_MODULE": "none",
                     "CLUSTER_HOSTNAME": "node1",
                     "PERSISTENCE_DATA_PATH": "./data"},
    )
    time.sleep(0.5)
    wclient = weaviate.Client('http://localhost:8080')
    test_class_name = "TestWillBeRemoved"
    retries = 3
    while retries > 0:
        try:
            wclient.schema.create_class({"class": test_class_name})
            wclient.schema.delete_class(test_class_name)
            retries -= 1
        except Exception as e:
            logging.info(f"error connecting to weaviate with client. Retrying in 1 second. Exception: {e}")
            time.sleep(1)
    yield wclient
    client.containers.get(container_name).remove(force=True)


def test_string_arrays(spark: SparkSession, weaviate_client: weaviate.Client):
    article = {"class": "Article",
               "properties": [
                   {"name": "title", "dataType": ["string"]},
                   {"name": "keywords", "dataType": ["string[]"]},
                   {"name": "keywords2", "dataType": ["string[]"]},
                   {"name": "keywords3", "dataType": ["string[]"]},
                   {"name": "title2", "dataType": ["string"]},
                   {"name": "title3", "dataType": ["string"]},
                   {"name": "title4", "dataType": ["string"]},
                   {"name": "title5", "dataType": ["string"]},
                   {"name": "title6", "dataType": ["string"]},
               ]}
    weaviate_client.schema.create_class(article)

    spark_schema = StructType([
        StructField('title', StringType(), True),
        StructField('keywords', ArrayType(StringType()), True),
        StructField('keywords2', ArrayType(StringType()), True),
        StructField('keywords3', ArrayType(StringType()), True),
        StructField('title2', StringType(), True),
        StructField('title3', StringType(), True),
        StructField('title4', StringType(), True),
        StructField('title5', StringType(), True),
        StructField('title6', StringType(), True),
    ])
    articles = [
        ("Sam and Sam", ["keyword1", "keyword2"], [""], [""], "", "", "", "", ""),
        ("", [], [""], [""], "", "", "", "", ""),
    ]
    df = spark.createDataFrame(data=articles, schema=spark_schema)
    df.write.format("io.weaviate.spark.Weaviate") \
        .option("scheme", "http") \
        .option("host", "20.232.49.89:80") \
        .option("className", "Article") \
        .mode("append").save()

    weaviate_articles = weaviate_client.data_object.get(class_name="Article").get("objects")
    weaviate_articles.sort(key=lambda x: x["properties"]["title"], reverse=True)
    assert len(weaviate_articles) == 2
    assert weaviate_articles[0]["properties"]["title"] == "Sam and Sam"
    assert weaviate_articles[0]["properties"]["keywords"] == articles[0][1]
    assert weaviate_articles[1]["properties"]["title"] == ""
    print(articles[1])
    assert weaviate_articles[1]["properties"]["keywords"] == articles[1][1]


def test_null_values(spark: SparkSession, weaviate_client: weaviate.Client):
    article = {"class": "Article",
               "properties": [
                   {"name": "title", "dataType": ["string"]},
                   {"name": "keywords", "dataType": ["string[]"]},
                   {"name": "keywords2", "dataType": ["string[]"]},
                   {"name": "double", "dataType": ["number"]},
                   {"name": "doubleArray", "dataType": ["number[]"]},
                   {"name": "integer", "dataType": ["int"]},
                   {"name": "integerArray", "dataType": ["int[]"]},
                   {"name": "bool", "dataType": ["boolean"]},
                   {"name": "date", "dataType": ["date"]},
               ]}
    weaviate_client.schema.create_class(article)

    spark_schema = StructType([
        StructField('title', StringType(), True),
        StructField('keywords', ArrayType(StringType()), True),
        StructField('double', DoubleType(), True),
        StructField('doubleArray', ArrayType(DoubleType()), True),
        StructField('integer', IntegerType(), True),
        StructField('integerArray', ArrayType(IntegerType()), True),
        StructField('bool', BooleanType(), True),
        StructField('date', DateType(), True),
        StructField('keywords2', ArrayType(StringType()), True),
    ])
    articles = [
        (None, [None], None, None, None, None, None, None, []),
        (None, [None], None, None, None, None, None, None, []),
    ]
    df = spark.createDataFrame(data=articles, schema=spark_schema)
    df.write.format("io.weaviate.spark.Weaviate") \
        .option("scheme", "http") \
        .option("host", "20.232.49.89:80") \
        .option("className", "Article") \
        .mode("append").save()

    weaviate_articles = weaviate_client.data_object.get(class_name="Article").get("objects")
    assert len(weaviate_articles) == 2
    article = weaviate_articles[1]
    assert article["properties"]["title"] == ""
    assert article["properties"]["keywords"] == [""]
    assert article["properties"]["keywords2"] == []
    assert article["properties"]["double"] == 0.0
    assert article["properties"]["doubleArray"] == []
    assert article["properties"]["integer"] == 0
    assert article["properties"]["integerArray"] == []
    assert article["properties"]["bool"] == False
    assert article["properties"]["date"] == "1970-01-01T00:00:00Z"


def test_id_column(spark: SparkSession, weaviate_client: weaviate.Client):
    article = {"class": "Article",
               "properties": [
                   {"name": "title", "dataType": ["string"]},
               ]}
    weaviate_client.schema.create_class(article)

    spark_schema = StructType([
        StructField('id', StringType(), True),
        StructField('title', StringType(), True),
    ])
    article1_id = str(uuid.UUID(int=1))
    articles = [(article1_id, "Sam and Sam")]
    df = spark.createDataFrame(data=articles, schema=spark_schema)
    df.write.format("io.weaviate.spark.Weaviate") \
        .option("scheme", "http") \
        .option("host", "20.232.49.89:80") \
        .option("className", "Article") \
        .option("id", "id") \
        .mode("append").save()

    weaviate_articles = weaviate_client.data_object.get(class_name="Article").get("objects")
    assert len(weaviate_articles) == 1
    assert weaviate_articles[0]["id"] == article1_id

    article2_id = uuid.UUID(int=2)
    articles = [(article2_id, "Sam and Sam")]
    with pytest.raises(py4j.protocol.Py4JJavaError):
        df = spark.createDataFrame(data=articles, schema=spark_schema)
        df.write.format("io.weaviate.spark.Weaviate") \
            .option("scheme", "http") \
            .option("host", "20.232.49.89:80") \
            .option("className", "Article") \
            .option("id", "id") \
            .mode("append").save()


movie_spark_schema = StructType([
    StructField('movie_id', DoubleType(), True),
    StructField('best_rating', DoubleType(), True),
    StructField('worst_rating', DoubleType(), True),
    StructField('url', StringType(), True),
    StructField('title', StringType(), True),
    StructField('poster_link', StringType(), True),
    StructField('genres', StringType(), True),
    StructField('actors', StringType(), True),
    StructField('director', StringType(), True),
    StructField('description', StringType(), True),
    StructField('date_published', StringType(), True),
    StructField('keywords', StringType(), True),
    StructField('rating_value', DoubleType(), True),
    StructField('review_aurthor', StringType(), True),
    StructField('review_date', StringType(), True),
    StructField('review_body', StringType(), True),
    StructField('duration', StringType(), True),
])


def test_large_movie_dataset(spark: SparkSession, weaviate_client: weaviate.Client):
    weaviate_client.schema.create_class(movie_schema)
    basepath = path.dirname(__file__)
    movies_csv_path = path.abspath(path.join(basepath, "movies.csv"))
    data = pd.read_csv(movies_csv_path, engine="python", on_bad_lines='skip')
    movies = []
    for i in range(0, len(data)):
        item = data.iloc[i]
        movies.append({
            'movie_id': float(item['id']),
            'url': str(item['url']),
            'title': str(item['Name']).lower(),
            'poster_link': str(item['PosterLink']),
            'genres': str(item['Genres']),
            'actors': str(item['Actors']).lower(),
            'director': str(item['Director']).lower(),
            'description': str(item['Description']),
            'date_published': str(item['DatePublished']),
            'keywords': str(item['Keywords']),
            'worst_rating': float(item['WorstRating']),
            'best_rating': float(item['BestRating']),
            'rating_value': float(item['RatingValue']),
            'review_aurthor': str(item['ReviewAurthor']),
            'review_body': str(item['ReviewBody']),
            'review_date': str(item['ReviewDate']),
            'duration': str(item['duration'])
        })

    df = spark.createDataFrame(data=movies, schema=movie_spark_schema)
    df.write.format("io.weaviate.spark.Weaviate") \
        .option("scheme", "http") \
        .option("host", "20.232.49.89:80") \
        .option("className", "Movies") \
        .mode("append").save()

    result = weaviate_client.query.aggregate("Movies").with_meta_count().do()
    print(result)
    assert result["data"]["Aggregate"]["Movies"][0]["meta"]["count"] == len(movies)


@pytest.fixture
def kafka_host():
    kafka = KafkaContainer()
    kafka.start()
    yield kafka.get_bootstrap_server()
    kafka.stop(force=True)


def test_kafka_streaming(spark: SparkSession, weaviate_client: weaviate.Client, tmp_path, kafka_host):
    article = {"class": "Article",
               "properties": [
                   {"name": "title", "dataType": ["string"]},
                   {"name": "keywords", "dataType": ["string[]"]},
                   {"name": "bool", "dataType": ["boolean"]},
                   {"name": "someint", "dataType": ["int"]},
                   {"name": "anotherString", "dataType": ["string"]},
                   {"name": "floatyfloat", "dataType": ["number"]},
               ]}
    weaviate_client.schema.create_class(article)
    producer = KafkaProducer(bootstrap_servers=[kafka_host],
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    for _ in range(100):
        kafka_result = producer.send('weaviate-test', {
            "title": "Sam", "keywords": ["article"], "bool": True, "someint": 1, "anotherString": "xx",
            "floatyfloat": 0.12345,
        })
        kafka_result.get(timeout=60)

    spark_schema = StructType([
        StructField('title', StringType(), True),
        StructField('keywords', ArrayType(StringType()), True),
        StructField('bool', BooleanType(), True),
        StructField('someint', IntegerType(), True),
        StructField('anotherString', StringType(), True),
        StructField('floatyfloat', DoubleType(), True),
    ])
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_host) \
        .option("subscribe", "weaviate-test") \
        .option("startingOffsets", "earliest") \
        .option("batchSize", 105) \
        .load() \
        .select(from_json(col("value").cast("string"), spark_schema).alias("parsed_value")) \
        .select(col("parsed_value.*"))

    write_stream = (
        df.writeStream
        .format("io.weaviate.spark.Weaviate")
        .option("scheme", "http")
        .option("host", "20.232.49.89:80")
        .option("className", "Article")
        .option("checkpointLocation", tmp_path.absolute())
        .outputMode("append")
        .start()
    )
    write_stream.processAllAvailable()
    result = weaviate_client.query.aggregate("Article").with_meta_count().do()
    assert result["data"]["Aggregate"]["Article"][0]["meta"]["count"] == 100
