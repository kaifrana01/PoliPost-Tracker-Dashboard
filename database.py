"""
database.py - SQLite database setup and models
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'news_dashboard.db')


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database with required tables."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT NOT NULL,
            description TEXT,
            url         TEXT UNIQUE,
            source_name TEXT,
            platform    TEXT,
            keyword     TEXT,
            published_at TEXT,
            fetched_at  TEXT DEFAULT (datetime('now')),
            author      TEXT,
            sentiment   REAL DEFAULT 0.0
        );

        CREATE INDEX IF NOT EXISTS idx_keyword     ON articles(keyword);
        CREATE INDEX IF NOT EXISTS idx_platform    ON articles(platform);
        CREATE INDEX IF NOT EXISTS idx_published   ON articles(published_at);

        CREATE TABLE IF NOT EXISTS fetch_logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            platform   TEXT,
            keyword    TEXT,
            status     TEXT,
            count      INTEGER DEFAULT 0,
            message    TEXT,
            fetched_at TEXT DEFAULT (datetime('now'))
        );
    """)

    conn.commit()
    conn.close()
    print(f"[DB] Initialized at {DB_PATH}")


def insert_article(article: dict):
    """Insert a single article (ignore duplicates by URL)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO articles
                (title, description, url, source_name, platform, keyword, published_at, author)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            article.get('title', ''),
            article.get('description', ''),
            article.get('url', ''),
            article.get('source_name', ''),
            article.get('platform', ''),
            article.get('keyword', ''),
            article.get('published_at', datetime.utcnow().isoformat()),
            article.get('author', ''),
        ))
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def insert_many_articles(articles: list):
    """Bulk insert articles."""
    inserted = 0
    for a in articles:
        inserted += insert_article(a)
    return inserted


def log_fetch(platform, keyword, status, count=0, message=''):
    conn = get_connection()
    conn.execute("""
        INSERT INTO fetch_logs (platform, keyword, status, count, message)
        VALUES (?, ?, ?, ?, ?)
    """, (platform, keyword, status, count, message))
    conn.commit()
    conn.close()