-- 1. List all tables
SELECT name FROM sqlite_master WHERE type='table';

-- 2. List top 10 movies by rating
SELECT title, year, imdb_rating
FROM movies_enriched
ORDER BY imdb_rating DESC
LIMIT 10;

-- 3. Count how many movies have OMDb enrichment
SELECT COUNT(*) FROM movies_enriched;

-- 4. List movies with missing ratings
SELECT movieId, title FROM movies_enriched WHERE imdb_rating IS NULL;

-- 5. Show movies released after year 2000 with ratings
SELECT title, year, imdb_rating
FROM movies_enriched
WHERE year >= 2000
ORDER BY imdb_rating DESC;
