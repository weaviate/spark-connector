# Weaviate Spark Connector
For use in Spark ETLs that populate Weaviate vector databases.

## Trying it out in Docker
```
sbt assembly
docker build -t spark-with-weaviate .
docker run -it spark-with-weaviate /opt/spark/bin/spark-shell
val ds = spark.range(100)
ds.write.format("io.weaviate.spark.Weaviate").save()
```
