#!/usr/bin/env python3
"""
etl.py
Simple MovieLens -> SQLite ETL with OMDb enrichment (mock fallback, cache).
Usage:
  python etl.py --use-omdb
  python etl.py --omdb-key YOURKEY
  python etl.py --mock-omdb
"""
import argparse
import os
import json
import time
import sqlite3
import csv
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

BASE = Path.cwd()
DATA_DIR = BASE / "ml-latest-small"
CACHE_FILE = BASE / "omdb_cache.json"
DB_FILE = BASE / "movies.db"
LOG_FILE = BASE / "run_log.txt"

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

def load_cache():
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf8"))
        except Exception:
            return {}
    return {}

def save_cache(cache):
    CACHE_FILE.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf8")

def read_csv_files():
    movies_path = DATA_DIR / "movies.csv"
    ratings_path = DATA_DIR / "ratings.csv"
    if not movies_path.exists() or not ratings_path.exists():
        raise FileNotFoundError("movies.csv or ratings.csv not found in ml-latest-small/")
    movies = pd.read_csv(movies_path)
    ratings = pd.read_csv(ratings_path)
    return movies, ratings

def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies (
      movieId INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      year INTEGER,
      genres TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
      userId INTEGER,
      movieId INTEGER,
      rating REAL,
      timestamp INTEGER,
      PRIMARY KEY(userId, movieId, timestamp),
      FOREIGN KEY(movieId) REFERENCES movies(movieId)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies_enriched (
      movieId INTEGER PRIMARY KEY,
      title TEXT NOT NULL,
      year INTEGER,
      genres TEXT,
      director TEXT,
      plot TEXT,
      box_office TEXT,
      imdb_rating REAL,
      imdb_id TEXT,
      other_fields TEXT
    );
    """)
    conn.commit()

def upsert_movies(conn, movies_df):
    cur = conn.cursor()
    rows = []
    for _, r in movies_df.iterrows():
        # movies.csv title format "Toy Story (1995)"
        title = str(r['title'])
        year = None
        # attempt to extract year
        if title.strip().endswith(")"):
            try:
                year_part = title.strip()[-5:-1]
                year = int(year_part)
            except Exception:
                year = None
        rows.append((int(r['movieId']), title, year if year is not None else None, r.get('genres', None)))
    cur.executemany("INSERT OR REPLACE INTO movies(movieId,title,year,genres) VALUES (?,?,?,?)", rows)
    conn.commit()
    log(f"Upserted {len(rows)} movies into movies table")

def upsert_ratings(conn, ratings_df):
    cur = conn.cursor()
    rows = []
    for _, r in ratings_df.iterrows():
        rows.append((int(r['userId']), int(r['movieId']), float(r['rating']), int(r['timestamp'])))
    # insert in batches to avoid huge single transaction
    cur.executemany("INSERT OR IGNORE INTO ratings(userId,movieId,rating,timestamp) VALUES (?,?,?,?)", rows)
    conn.commit()
    log(f"Inserted {len(rows)} ratings into ratings table (duplicates ignored)")

def query_omdb(title, year, api_key):
    """
    Query OMDb by title and year. Returns parsed JSON dict or None.
    """
    base = "http://www.omdbapi.com/"
    params = {"t": title}
    if year:
        params["y"] = str(year)
    if api_key:
        params["apikey"] = api_key
    r = requests.get(base, params=params, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"OMDb HTTP {r.status_code}")
    return r.json()

def enrich_movies(conn, api_key=None, mock_cache=None):
    cache = load_cache()
    cur = conn.cursor()
    cur.execute("SELECT movieId, title, year, genres FROM movies")
    rows = cur.fetchall()
    processed = 0
    hits = 0
    misses = 0
    for movieId, title, year, genres in rows:
        key = f"{title}|{year}"
        if key in cache:
            data = cache[key]
            hits += 1
        else:
            if mock_cache is not None:
                # use mock mapping if provided
                data = mock_cache.get(key)
            else:
                if not api_key:
                    log(f"No API key provided and no mock entry for {key} -> skipping")
                    continue
                try:
                    data = query_omdb(title, year, api_key)
                except Exception as e:
                    log(f"OMDb request failed for {title} ({year}): {e}")
                    data = None
            cache[key] = data
            misses += 1
            # save incrementally to avoid loss on long runs
            save_cache(cache)
            time.sleep(0.1)
        if data and data.get("Response","True") != "False":
            director = data.get("Director")
            plot = data.get("Plot")
            box_office = data.get("BoxOffice")
            imdb_rating = None
            try:
                imdb_rating = float(data.get("imdbRating")) if data.get("imdbRating") and data.get("imdbRating")!="N/A" else None
            except Exception:
                imdb_rating = None
            imdb_id = data.get("imdbID")
            other = {k:v for k,v in data.items() if k not in ("Title","Year","Director","Plot","BoxOffice","imdbRating","imdbID","Genre")}
            cur.execute("""
                INSERT OR REPLACE INTO movies_enriched(movieId,title,year,genres,director,plot,box_office,imdb_rating,imdb_id,other_fields)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                movieId,
                title,
                year,
                genres,
                director,
                plot,
                box_office,
                imdb_rating,
                imdb_id,
                json.dumps(other, ensure_ascii=False)
            ))
            conn.commit()
            processed += 1
    save_cache(cache)
    log(f"Enrichment done. Processed:{processed} cache_hits:{hits} cache_misses:{misses}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--omdb-key", dest="omdb_key", help="OMDb API key")
    parser.add_argument("--use-omdb", action="store_true", help="Use OMDb (read from .env if present)")
    parser.add_argument("--mock-omdb", action="store_true", help="Run using mock cache only (no real API calls)")
    args = parser.parse_args()

    load_dotenv()
    env_key = os.getenv("OMDB_API_KEY")
    omdb_key = args.omdb_key or (env_key if args.use_omdb else None)

    if args.use_omdb and not omdb_key:
        log("Warning: --use-omdb requested but OMDB_API_KEY not found in .env or --omdb-key not provided")

    # read data
    log("Starting ETL")
    try:
        movies_df, ratings_df = read_csv_files()
    except Exception as e:
        log(f"Error reading CSVs: {e}")
        return

    conn = sqlite3.connect(str(DB_FILE))
    create_schema(conn)
    upsert_movies(conn, movies_df)
    upsert_ratings(conn, ratings_df)

    # load mock cache if present
    mock_cache = None
    if args.mock_omdb:
        try:
            mock_cache = load_cache()
            log("Running in mock-omdb mode; loaded mock cache")
        except Exception:
            mock_cache = {}
    # run enrichment
    if args.mock_omdb or omdb_key:
        enrich_movies(conn, api_key=omdb_key if not args.mock_omdb else None, mock_cache=mock_cache)
    else:
        log("Skipping enrichment (no OMDb key and not mock mode)")

    # Export sample top10
    try:
        df_enriched = pd.read_sql_query("SELECT * FROM movies_enriched ORDER BY imdb_rating DESC NULLS LAST LIMIT 10;", conn)
        out = BASE / "sample_output"
        out.mkdir(exist_ok=True)
        df_enriched.to_csv(out / "top10_enriched.csv", index=False)
        log("Exported sample_output/top10_enriched.csv")
    except Exception as e:
        log(f"Could not export sample_output: {e}")

    conn.close()
    log("ETL finished")
if __name__ == "__main__":
    main()
