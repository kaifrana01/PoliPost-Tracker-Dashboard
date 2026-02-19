"""
data_fetcher.py
===============
Fetches news articles from multiple sources for predefined keywords.

Sources used (all free / open RSS):
  - Google News RSS (via news.google.com)
  - Yahoo News RSS
  - Bing News RSS  (Microsoft Edge uses Bing)
  - Mozilla / Firefox default search → DuckDuckGo RSS
  - Opera News RSS
  - Safari (Apple News) → Reuters / AP RSS feeds

NewsAPI is also supported (set NEWSAPI_KEY env-variable).
"""

import os
import re
import time
import hashlib
import feedparser
import requests
from datetime import datetime
from urllib.parse import quote_plus

from database import insert_many_articles, log_fetch

# ──────────────────────────────────────────────────────────────────────────────
# PREDEFINED KEYWORDS
# ──────────────────────────────────────────────────────────────────────────────
KEYWORDS = [
    "politics modi",
    "pm modi",
    "rahul gandhi",
    "Parliament",
    "Policy",
    "Government",
    "Human Rights",
]

# ──────────────────────────────────────────────────────────────────────────────
# PLATFORM DEFINITIONS  (name → RSS URL template)
# ──────────────────────────────────────────────────────────────────────────────
PLATFORMS = {
    "Google News": "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en",
    "Yahoo News":  "https://news.yahoo.com/rss/search?p={query}",
    "Bing News":   "https://www.bing.com/news/search?q={query}&format=rss",
    "DuckDuckGo":  "https://duckduckgo.com/?q={query}&iar=news&format=rss",   # Firefox default engine
    "Opera News":  "https://news.google.com/rss/search?q={query}&hl=en&gl=US&ceid=US:en",  # Opera uses Google
    "Reuters":     "https://feeds.reuters.com/reuters/INtopNews",             # Safari / Apple News source
}

# NewsAPI (optional – set NEWSAPI_KEY in environment)
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _parse_date(date_str: str) -> str:
    """Try to parse various date formats to ISO-8601."""
    if not date_str:
        return datetime.utcnow().isoformat()
    # feedparser already gives a struct_time in .published_parsed
    try:
        import email.utils
        parsed = email.utils.parsedate_to_datetime(date_str)
        return parsed.isoformat()
    except Exception:
        pass
    return date_str


def _clean(text: str) -> str:
    """Strip HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _make_url(url: str, query: str) -> str:
    return url.replace("{query}", quote_plus(query))


# ──────────────────────────────────────────────────────────────────────────────
# FETCHERS
# ──────────────────────────────────────────────────────────────────────────────

def fetch_rss(platform: str, url_template: str, keyword: str) -> list:
    """Fetch & parse an RSS feed for a given keyword."""
    articles = []
    url = _make_url(url_template, keyword)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        feed = feedparser.parse(resp.content)
        for entry in feed.entries:
            pub_raw = getattr(entry, "published", "")
            if not pub_raw and hasattr(entry, "published_parsed") and entry.published_parsed:
                import calendar
                pub_raw = datetime.utcfromtimestamp(
                    calendar.timegm(entry.published_parsed)
                ).isoformat()

            articles.append({
                "title":        _clean(entry.get("title", "")),
                "description":  _clean(entry.get("summary", "")),
                "url":          entry.get("link", ""),
                "source_name":  feed.feed.get("title", platform),
                "platform":     platform,
                "keyword":      keyword,
                "published_at": _parse_date(pub_raw),
                "author":       entry.get("author", ""),
            })
    except Exception as exc:
        log_fetch(platform, keyword, "error", 0, str(exc))
        print(f"[WARN] {platform} | {keyword} → {exc}")
        return []

    log_fetch(platform, keyword, "ok", len(articles))
    print(f"[OK]   {platform} | {keyword} → {len(articles)} articles")
    return articles


def fetch_newsapi(keyword: str) -> list:
    """Fetch from NewsAPI (requires NEWSAPI_KEY)."""
    if not NEWSAPI_KEY:
        return []
    articles = []
    try:
        url = (
            f"https://newsapi.org/v2/everything?"
            f"q={quote_plus(keyword)}&language=en&sortBy=publishedAt"
            f"&pageSize=20&apiKey={NEWSAPI_KEY}"
        )
        data = requests.get(url, headers=HEADERS, timeout=15).json()
        for item in data.get("articles", []):
            articles.append({
                "title":        item.get("title", ""),
                "description":  item.get("description", ""),
                "url":          item.get("url", ""),
                "source_name":  item.get("source", {}).get("name", "NewsAPI"),
                "platform":     "NewsAPI",
                "keyword":      keyword,
                "published_at": item.get("publishedAt", ""),
                "author":       item.get("author", ""),
            })
    except Exception as exc:
        print(f"[WARN] NewsAPI | {keyword} → {exc}")
    return articles


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────────────────

def fetch_all(keywords=None, platforms=None) -> dict:
    """
    Fetch articles for all keywords from all platforms.
    Returns summary dict { keyword: count }.
    """
    if keywords is None:
        keywords = KEYWORDS
    if platforms is None:
        platforms = PLATFORMS

    summary = {}
    for keyword in keywords:
        total = 0
        for platform, url_template in platforms.items():
            arts = fetch_rss(platform, url_template, keyword)
            total += insert_many_articles(arts)
            time.sleep(0.5)   # be polite to RSS servers

        # Optional NewsAPI
        na_arts = fetch_newsapi(keyword)
        total += insert_many_articles(na_arts)

        summary[keyword] = total
        print(f"[DONE] Keyword '{keyword}' → {total} new articles stored\n")

    return summary


if __name__ == "__main__":
    from database import init_db
    init_db()
    result = fetch_all()
    print("\n=== Fetch Summary ===")
    for k, v in result.items():
        print(f"  {k:30s} → {v} new")