-- Fichier: init.cql

CREATE KEYSPACE IF NOT EXISTS reddit_db 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reddit_db;

CREATE TABLE IF NOT EXISTS viral_posts (
    id text PRIMARY KEY,
    author text,
    subreddit text,
    text_content text,
    sentiment text,
    sujet text,
    score_predit float,
    viralite text,
    creation_date timestamp
);