from pyspark.sql.types import *


event_schema = {
    "class": "Event",
    "description": "A page on the internet",
    "properties": [
        #                {
        #                    "dataType": ["string"],
        #                    "description": "Domain of the page",
        #                    "name": "domain",
        #                },
        #                {
        #                    "dataType": ["string"],
        #                    "description": "URL of the page",
        #                    "name": "og_url",
        #                },
        #                {
        #                    "dataType": ["string"],
        #                    "description": "Title of the page",
        #                    "name": "title"
        #                },
        {
            "dataType": ["string"],
            "description": "MONEY",
            "name": "moneyline"
        },
        #                {
        #                    "dataType": ["string"],
        #                    "description": "time left",
        #                    "name": "game_clock"
        #                },
        {
            "dataType": ["string"],
            "description": "current period",
            "name": "current_period",
        },
        {
            "dataType": ["boolean"],
            "description": "is_live",
            "name": "is_live",
        },
        # {
        #                    "dataType": ["string"],
        #                    "description": "over_under",
        #                    "name": "over_under"
        #                },
        {
            "dataType": ["string[]"],
            "description": "lotta plays",
            "name": "plays"
        },
        {
            "dataType": ["string[]"],
            "description": "lotta scores",
            "name": "scores"
        },
        {
            "dataType": ["string[]"],
            "description": "lotta teams",
            "name": "teams"
        }
    ]
}


spark_event_schema = StructType([
#    StructField("domain", StringType()),
#    StructField("og_url", StringType()),
#    StructField("title", StringType()),
    StructField("scores", ArrayType(StringType())),
    StructField("teams", ArrayType(StringType())),
    StructField("moneyline", StringType()),
    StructField("id_column", StringType()),
#    StructField("over_under", StringType()),
    StructField("current_period", StringType()),
#    StructField("game_clock", StringType()),
    StructField("is_live", BooleanType()),
#    StructField("show_game", BooleanType()),
#    StructField("game_id", StringType()),
    StructField("plays", ArrayType(StringType())),
#    StructField("hardcoded", ArrayType(StringType())),
#    StructField("search_terms", ArrayType(StringType())),
])