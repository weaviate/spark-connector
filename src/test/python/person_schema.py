from pyspark.sql.types import *


person_schema = {
    "class": "Person",
    "properties": [
        {
            "dataType": ["text"],
            "name": "firstName"
        },
        {
            "dataType": ["text"],
            "name": "lastName",
        },
        {
            "dataType": ["boolean"],
            "name": "isAlive",
        },
        {
            "dataType": ["text"],
            "name": "profession"
        },
        {
            "dataType": ["text[]"],
            "name": "description"
        },
        {
            "dataType": ["object"],
            "name": "address",
            "nestedProperties": [
                {"dataType": ["text"], "name": "city"},
                {"dataType": ["text"], "name": "state"},
                {"dataType": ["int"], "name": "zipcode"},
                {"dataType": ["text"], "name": "country"}
            ]
        },
        {
            "dataType": ["object[]"],
            "name": "phoneNumbers",
            "nestedProperties": [
                {"dataType": ["text"], "name": "number"},
                {"dataType": ["text"], "name": "country"}
            ]
        }
    ]
}


spark_person_schema = StructType([
    StructField("id_column", StringType()),
    StructField("firstName", StringType()),
    StructField("lastName", StringType()),
    StructField("isAlive", BooleanType()),
    StructField("profession", StringType()),
    StructField("description", ArrayType(StringType())),
    StructField("address", StructType([StructField("city", StringType()), StructField("state", StringType()), StructField("zipcode", IntegerType()), StructField("country", StringType())])),
    StructField("phoneNumbers", ArrayType(StructType([StructField("number", StringType()), StructField("country", StringType())]))),
])
