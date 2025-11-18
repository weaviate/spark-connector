from pyspark.sql.types import *


byov_schema = {
    "class": "BringYourOwnVector",
    "description": "Collection to test bringing your own vectors",
    "properties": [
        {
            "dataType": ["text"],
            "name": "title"
        },
    ]
}


spark_byov_schema = StructType([
    StructField("title", StringType()),
    StructField("id_column", StringType()),
    StructField("embedding", ArrayType(FloatType()))
])
