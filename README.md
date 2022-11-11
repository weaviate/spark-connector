# Weaviate Spark Connector
For use in Spark ETLs that populate Weaviate vector databases.

## Trying it out in Docker
```
sbt assembly
docker build -t spark-with-weaviate .
docker run -it spark-with-weaviate /opt/spark/bin/spark-shell
case class Article (title: String, content: String)
val articles = Seq( Article("Sam", "Sam"))
val ds = articles.toDF
ds.write.format("io.weaviate.spark.Weaviate").mode("append").save()
```
