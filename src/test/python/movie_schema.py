movie_schema = {
    "class": "Movies",
    "description": "A collection of movies with title, description, director, actors, rating, etc.",
    "properties": [
        {
            "name": "movie_id",
            "dataType": ["number"],
            "description": "The id of the movie",
        },
        {
            "name": "best_rating",
            "dataType": ["number"],
            "description": "best rating of the movie",
        },
        {
            "name": "worst_rating",
            "dataType": ["number"],
            "description": "worst rating of the movie",
        },
        {
            "name": "url",
            "dataType": ["string"],
            "description": "The IMBD url of the movie",
        },
        {
            "name": "title",
            "dataType": ["string"],
            "description": "The name of the movie",
        },
        {
            "name": "poster_link",
            "dataType": ["string"],
            "description": "The poster link of the movie",
        },
        {
            "name": "genres",
            "dataType": ["string"],
            "description": "The genres of the movie",
        },
        {
            "name": "actors",
            "dataType": ["string"],
            "description": "The actors of the movie",
        },
        {
            "name": "director",
            "dataType": ["string"],
            "description": "Director of the movie",
        },
        {
            "name": "description",
            "dataType": ["string"],
            "description": "overview of the movie",
        },
        {
            "name": "date_published",
            "dataType": ["string"],
            "description": "The date on which movie was published",
        },
        {
            "name": "keywords",
            "dataType": ["string"],
            "description": "main keywords of the movie",
        },
        {
            "name": "rating_value",
            "dataType": ["number"],
            "description": "rating value of the movie",
        },
        {
            "name": "review_aurthor",
            "dataType": ["string"],
            "description": "aurthor of the review",
        },
        {
            "name": "review_date",
            "dataType": ["string"],
            "description": "date of review",
        },
        {
            "name": "review_body",
            "dataType": ["string"],
            "description": "body of the review",
        },
        {
            "name": "duration",
            "dataType": ["string"],
            "description": "the duration of the review",
        }
    ]
}