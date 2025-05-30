# Weaviate Spark Connector
For use in [Spark](https://spark.apache.org/docs/latest/) jobs to populate a Weaviate vector database.

## Installation
You can choose one of the following options to install the Weaviate Spark Connector:

### Download JAR from GitHub
You can download the latest JAR from [GitHub releases here](https://github.com/weaviate/spark-connector/releases/latest).

### Building the JAR yourself
To use in your own Spark job you will first need to build the fat jar of the package by running
`sbt assembly` which will create the artifact in `./target/scala-2.12/spark-connector-assembly-1.2.8.jar`

### Using the JAR in Spark
You can configure spark-shell or tools like spark-submit to use the JAR like this:
```shell
spark-shell --jars spark-connector-assembly-1.4.0.jar
```

### Using the JAR in Databricks
To run on Databricks simply upload the jar file to your cluster in the libraries tab as in the below image.
<img src="readme-assets/install-image.png">

After installation your cluster page should look something like this.
<img src="readme-assets/libraries-image.png">

### Using Maven Central Repository
You can also use Maven to include the Weaviate Spark Connector as dependency in your
Spark application. See [here](https://mvnrepository.com/artifact/io.weaviate/spark-connector).

### Using sbt

Running cross versions tests:
```shell
sbt -v +test
```

Building `Scala 2.12` and `Scala 2.13` binaries:
```shell
sbt +assembly
```

## Usage

First create a schema in Weaviate as the connector will not create one automatically. See the [tutorial](https://weaviate.io/developers/weaviate/tutorials/spark-connector#writing-to-weaviate) for how to do this.

Afterwards loading data from Spark is as easy as this!

```python
(
    my_df
    .write
    .format("io.weaviate.spark.Weaviate")
    .option("scheme", "http")
    .option("host", weaviate_host)
    .option("className", "MyClass")
    .mode("append")
    .save()
)
```

If you already have vectors available in your dataframe (recommended for batch insert performance) you can easily supply them with the vector option.
```python
(
    my_df
    .write
    .format("io.weaviate.spark.Weaviate")
    .option("scheme", "http")
    .option("host", weaviate_host)
    .option("className", "MyClass")
    .option("vector", vector_column_name)
    .mode("append")
    .save()
)
```

By default the Weaviate client will create document IDs for you for new documents but if you already have IDs you
can also supply those in the dataframe.
```python
(
    my_df
    .write
    .format("io.weaviate.spark.Weaviate")
    .option("scheme", "http")
    .option("host", weaviate_host)
    .option("className", "MyClass")
    .option("id", id_column_name)
    .mode("append")
    .save()
)
```

For authenticated clusters such as with [WCS](https://weaviate.io/developers/wcs) the `apiKey` option can be used. Further options including OIDC and custom headers are listed in the [tutorial](https://weaviate.io/developers/weaviate/tutorials/spark-connector#spark-connector-options).

```python
(
    my_df
    .write
    .format("io.weaviate.spark.Weaviate")
    .option("scheme", "https")
    .option("host", "demo-endpoint.weaviate.network")
    .option("apiKey", WEAVIATE_API_KEY)
    .option("className", "MyClass")
    .option("batchSize", 100)
    .option("vector", vector_column_name)
    .option("id", id_column_name)
    .mode("append")
    .save()
)
```

### Write Modes
Currently only the append write mode is supported. We do not yet support upsert or 
error if exists write semantics.

Both batch operations and streaming writes are supported.

### Spark to Weaviate DataType mappings
The connector is able to automatically infer the correct Spark DataType based
on your schema for the class in Weaviate. Your DataFrame column name
needs to match the property name of your class in Weaviate. The table below
shows how the connector infers the DataType:

| Weaviate DataType | Spark DataType   | Notes |
|--|---|---|
|string  | StringType  |   |
|string[]  | Array[StringType]  |   |
|int  | IntegerType  | Weaviate only supports int32 for now. More info [here](https://github.com/weaviate/weaviate/issues/1563).  |
|int[]  |  Array[IntegerType] |   |
|boolean  | BooleanType  |    |
|boolean[]  | Array[BooleanType]  |  |
|number  | DoubleType  |   |
|number[]  | Array[DoubleType]  |   |
|date  | DateType  |   |
|date[]  | Array[DateType]  |   |
|text  | StringType  |   |
|text[]  | StringType  |   |
|geoCoordinates  | StringType  |   |
|phoneNumber  | StringType  |   |
|blob  | StringType  | Encode your blob as base64 string |
|vector  | Array[FloatType]  |   |
|cross reference  | string  | Not supported yet |

Please also take a look at the 
[Weaviate data types docs](https://weaviate.io/developers/weaviate/current/schema/datatypes.html) and the
[Spark DataType docs](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).

## Developer
### Compiling
This repository uses [SBT](https://www.scala-sbt.org/) to compile the code. SBT can be installed on MacOS
following the instructions [here](https://www.scala-sbt.org/1.x/docs/Setup.html).

You will also need Java 8+ and Scala 2.12 installed. The easiest way to get everything set up is to install IntelliJ.

To compile the package simply run `sbt compile` to ensure that you have everything needed to run the Spark connector.

### Running the Tests
The unit and integration tests can be run via `sbt test`. 

The integration tests stand up a local Weaviate instance running in docker and then run the 
Apache Spark code in a separate docker container. You will need to have docker running to run all tests.


## Trying it out Locally in Docker
```
sbt assembly
docker build -t spark-with-weaviate .
docker run -it spark-with-weaviate /opt/spark/bin/spark-shell
case class Article (title: String, content: String)
val articles = Seq( Article("Sam", "Sam")).toDF
articles.write.format("io.weaviate.spark.Weaviate")
  .option("scheme", "http")
  .option("host", "localhost:8080")
  .mode("append").save()
```
