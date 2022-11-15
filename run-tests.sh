#!/usr/bin/env bash

set -xe

spark_container="spark-with-weaviate"
weaviate_container="weaviate-test-will-be-removed"

function cleanup {
  set +e
  echo "Deleting containers that were created for testing"
  docker stop "$weaviate_container"
  docker rm "$weaviate_container"
  docker rm "$spark_container"
}
trap cleanup EXIT

sbt assembly
docker build -t spark-with-weaviate .
docker run -d --name="$weaviate_container" \
    -p 8080:8080 \
    -e QUERY_DEFAULTS_LIMIT=25 \
    -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED='true' \
    -e DEFAULT_VECTORIZER_MODULE='none' \
    -e CLUSTER_HOSTNAME='node1' \
    -e PERSISTENCE_DATA_PATH='./data' \
    semitechnologies/weaviate:1.16.1
# wait for weaviate to come up
curl --retry-all-errors --retry-connrefused --retry 5 http://localhost:8080/v1/schema

# Upload schema to weaviate for test
curl \
    -X POST \
    -H "Content-Type: application/json" \
    -d '{
        "class": "Article",
        "description": "A description of this class, in this case, it is about authors",
        "properties": [
            {
                "dataType": [
                    "string"
                ],
                "name": "title"
            },
            {
                "dataType": [
                    "int"
                ],
                "name": "wordCount"
            },
            {
                "dataType": [
                    "string"
                ],
                "name": "content"
            }
        ]
    }' http://localhost:8080/v1/schema

docker run --net=host --name "$spark_container" spark-with-weaviate /opt/spark/bin/spark-shell -i /opt/spark/example.scala

article=$(echo '{
    "query": "{
      Get {
        Article {
          title
          content
          wordCount
        }
      }
    }"
  }' | curl \
      -X POST \
      -H 'Content-Type: application/json' \
      -d @- \
      http://localhost:8080/v1/graphql)

title=$(echo "$article" | jq -r '.data.Get.Article[0].title')
content=$(echo "$article" | jq -r '.data.Get.Article[0].content')
wordCount=$(echo "$article" | jq -r '.data.Get.Article[0].wordCount')
count=$(echo "$article" | jq -r '.data.Get.Article | length')
[[ $count -eq 1 ]] && echo "Count check passed" || exit 1
[ "$title" == "Sam" ] && echo "Title check passed" || exit 1
[ "$content" == "Sam and Sam" ] && echo "Content check passed" || exit 1
[[ $wordCount -eq 3 ]] && echo "WordCount check passed" || exit 1
echo "All tests passed" && exit 0
