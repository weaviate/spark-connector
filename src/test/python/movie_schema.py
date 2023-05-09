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
            "dataType": ["text"],
            "description": "The IMBD url of the movie",
        },
        {
            "name": "title",
            "dataType": ["text"],
            "description": "The name of the movie",
        },
        {
            "name": "poster_link",
            "dataType": ["text"],
            "description": "The poster link of the movie",
        },
        {
            "name": "genres",
            "dataType": ["text"],
            "description": "The genres of the movie",
        },
        {
            "name": "actors",
            "dataType": ["text"],
            "description": "The actors of the movie",
        },
        {
            "name": "director",
            "dataType": ["text"],
            "description": "Director of the movie",
        },
        {
            "name": "description",
            "dataType": ["text"],
            "description": "overview of the movie",
        },
        {
            "name": "date_published",
            "dataType": ["text"],
            "description": "The date on which movie was published",
        },
        {
            "name": "keywords",
            "dataType": ["text"],
            "description": "main keywords of the movie",
        },
        {
            "name": "rating_value",
            "dataType": ["number"],
            "description": "rating value of the movie",
        },
        {
            "name": "review_aurthor",
            "dataType": ["text"],
            "description": "aurthor of the review",
        },
        {
            "name": "review_date",
            "dataType": ["text"],
            "description": "date of review",
        },
        {
            "name": "review_body",
            "dataType": ["text"],
            "description": "body of the review",
        },
        {
            "name": "duration",
            "dataType": ["text"],
            "description": "the duration of the review",
        }
    ]
}