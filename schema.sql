DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies_enriched;

CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER,
    genres TEXT
);

CREATE TABLE ratings (
    userId INTEGER,
    movieId INTEGER,
    rating REAL,
    timestamp INTEGER,
    PRIMARY KEY (userId, movieId)
);

CREATE TABLE movies_enriched (
    movieId INTEGER PRIMARY KEY,
    title TEXT,
    year INTEGER,
    genres TEXT,
    director TEXT,
    plot TEXT,
    box_office TEXT,
    imdb_rating REAL,
    imdb_id TEXT,
    other_fields TEXT
);
