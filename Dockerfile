FROM apache/spark:3.3.1

COPY ./target/scala-2.12/weaviate-spark-connector-assembly-0.1.0-SNAPSHOT.jar /opt/spark/jars
COPY example.scala-spark /opt/spark
