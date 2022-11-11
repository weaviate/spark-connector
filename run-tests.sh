#!/usr/bin/env bash

sbt assembly
docker build -t spark-with-weaviate .
docker run spark-with-weaviate /opt/spark/bin/spark-shell -i /opt/spark/example.scala

