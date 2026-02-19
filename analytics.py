"""
analytics.py
============
Performs data analysis using pandas & numpy on the stored articles.
Returns chart-ready JSON structures for the Flask dashboard.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from database import get_connection, DB_PATH


# ──────────────────────────────────────────────────────────────────────────────
# DATA LOADER
# ──────────────────────────────────────────────────────────────────────────────

def load_dataframe() -> pd.DataFrame:
    """Load all articles from SQLite into a pandas DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM articles ORDER BY published_at DESC", conn
    )
    conn.close()

    if df.empty:
        return df

    # Parse dates
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df["published_at"] = df["published_at"].dt.tz_localize(None)  # strip tz for simplicity
    df["date"]   = df["published_at"].dt.date
    df["week"]   = df["published_at"].dt.to_period("W").apply(lambda r: str(r.start_time.date()) if pd.notna(r) else None)
    df["month"]  = df["published_at"].dt.to_period("M").apply(lambda r: str(r) if pd.notna(r) else None)
    df["year"]   = df["published_at"].dt.year
    df["hour"]   = df["published_at"].dt.hour

    return df


# ──────────────────────────────────────────────────────────────────────────────
# SUMMARY STATS
# ──────────────────────────────────────────────────────────────────────────────

def get_summary(df: pd.DataFrame) -> dict:
    """Top-level KPI cards."""
    if df.empty:
        return {
            "total_posts": 0, "total_keywords": 0,
            "total_platforms": 0, "latest_fetch": "No data yet",
            "today_count": 0, "this_week_count": 0,
        }

    today = datetime.utcnow().date()
    week_start = today - timedelta(days=today.weekday())

    return {
        "total_posts":      int(len(df)),
        "total_keywords":   int(df["keyword"].nunique()),
        "total_platforms":  int(df["platform"].nunique()),
        "latest_fetch":     str(df["published_at"].max())[:16],
        "today_count":      int((df["date"] == today).sum()),
        "this_week_count":  int((df["date"] >= week_start).sum()),
    }


# ──────────────────────────────────────────────────────────────────────────────
# TIME-SERIES HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _series_to_chart(series: pd.Series, label: str) -> dict:
    """Convert a date-indexed Series to Chart.js labels/data dict."""
    series = series.sort_index()
    return {
        "labels": [str(i) for i in series.index],
        "datasets": [{
            "label":           label,
            "data":            [int(v) for v in series.values],
            "borderColor":     "#6366f1",
            "backgroundColor": "rgba(99,102,241,0.15)",
            "fill":            True,
            "tension":         0.4,
        }]
    }


def get_daily_trend(df: pd.DataFrame, days: int = 30) -> dict:
    if df.empty:
        return {"labels": [], "datasets": []}
    cutoff = datetime.utcnow().date() - timedelta(days=days)
    sub = df[df["date"] >= cutoff]
    series = sub.groupby("date").size()
    # fill missing dates
    idx = pd.date_range(cutoff, datetime.utcnow().date(), freq="D").date
    series = series.reindex(idx, fill_value=0)
    return _series_to_chart(series, "Daily Posts")


def get_weekly_trend(df: pd.DataFrame, weeks: int = 12) -> dict:
    if df.empty:
        return {"labels": [], "datasets": []}
    series = df.groupby("week").size().tail(weeks)
    return _series_to_chart(series, "Weekly Posts")


def get_monthly_trend(df: pd.DataFrame, months: int = 12) -> dict:
    if df.empty:
        return {"labels": [], "datasets": []}
    series = df.groupby("month").size().tail(months)
    return _series_to_chart(series, "Monthly Posts")


def get_yearly_trend(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"labels": [], "datasets": []}
    series = df.groupby("year").size()
    return _series_to_chart(series, "Yearly Posts")


# ──────────────────────────────────────────────────────────────────────────────
# PLATFORM ANALYSIS
# ──────────────────────────────────────────────────────────────────────────────

PLATFORM_COLORS = [
    "#6366f1", "#06b6d4", "#f59e0b", "#10b981",
    "#ef4444", "#8b5cf6", "#ec4899", "#14b8a6",
]

def get_platform_distribution(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"labels": [], "datasets": [{"data": [], "backgroundColor": []}]}
    counts = df["platform"].value_counts()
    return {
        "labels": counts.index.tolist(),
        "datasets": [{
            "data":            counts.values.tolist(),
            "backgroundColor": PLATFORM_COLORS[:len(counts)],
            "borderWidth":     2,
            "borderColor":     "#0f172a",
        }]
    }


def get_platform_keyword_heatmap(df: pd.DataFrame) -> dict:
    """Returns matrix data for keyword × platform heatmap."""
    if df.empty:
        return {"platforms": [], "keywords": [], "matrix": []}
    pivot = df.pivot_table(index="keyword", columns="platform",
                           values="id", aggfunc="count", fill_value=0)
    return {
        "keywords":  pivot.index.tolist(),
        "platforms": pivot.columns.tolist(),
        "matrix":    pivot.values.tolist(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# KEYWORD ANALYSIS
# ──────────────────────────────────────────────────────────────────────────────

def get_keyword_distribution(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"labels": [], "datasets": [{"data": [], "backgroundColor": []}]}
    counts = df["keyword"].value_counts()
    return {
        "labels": counts.index.tolist(),
        "datasets": [{
            "label":           "Articles",
            "data":            counts.values.tolist(),
            "backgroundColor": PLATFORM_COLORS[:len(counts)],
            "borderColor":     "#1e293b",
            "borderWidth":     1,
        }]
    }


def get_keyword_trends_over_time(df: pd.DataFrame, period: str = "daily") -> dict:
    """Multi-line chart: one line per keyword over time."""
    if df.empty:
        return {"labels": [], "datasets": []}

    group_col = "date" if period == "daily" else ("week" if period == "weekly" else "month")
    keywords  = df["keyword"].unique()
    pivot     = df.groupby([group_col, "keyword"]).size().unstack(fill_value=0)

    labels   = [str(i) for i in pivot.index]
    datasets = []
    for i, kw in enumerate(pivot.columns):
        datasets.append({
            "label":           kw,
            "data":            [int(v) for v in pivot[kw].values],
            "borderColor":     PLATFORM_COLORS[i % len(PLATFORM_COLORS)],
            "backgroundColor": "transparent",
            "tension":         0.4,
            "borderWidth":     2,
        })
    return {"labels": labels, "datasets": datasets}


# ──────────────────────────────────────────────────────────────────────────────
# HOURLY HEATMAP
# ──────────────────────────────────────────────────────────────────────────────

def get_hourly_distribution(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"labels": list(range(24)), "datasets": [{"data": [0]*24}]}
    counts = df.groupby("hour").size().reindex(range(24), fill_value=0)
    return {
        "labels": [f"{h:02d}:00" for h in range(24)],
        "datasets": [{
            "label":           "Articles per Hour",
            "data":            counts.values.tolist(),
            "backgroundColor": "rgba(99,102,241,0.7)",
            "borderRadius":    4,
        }]
    }


# ──────────────────────────────────────────────────────────────────────────────
# RECENT ARTICLES TABLE
# ──────────────────────────────────────────────────────────────────────────────

def get_recent_articles(df: pd.DataFrame, n: int = 50) -> list:
    if df.empty:
        return []
    cols = ["title", "source_name", "platform", "keyword", "published_at", "url"]
    sub  = df[cols].head(n).copy()
    sub["published_at"] = sub["published_at"].astype(str).str[:16]
    return sub.fillna("").to_dict(orient="records")


# ──────────────────────────────────────────────────────────────────────────────
# NUMPY STATS
# ──────────────────────────────────────────────────────────────────────────────

def get_numpy_stats(df: pd.DataFrame) -> dict:
    """Descriptive statistics using numpy."""
    if df.empty:
        return {}
    daily = df.groupby("date").size().values.astype(float)
    return {
        "mean_daily":   round(float(np.mean(daily)), 2),
        "std_daily":    round(float(np.std(daily)),  2),
        "max_daily":    int(np.max(daily)),
        "min_daily":    int(np.min(daily)),
        "median_daily": round(float(np.median(daily)), 2),
        "percentile_75": round(float(np.percentile(daily, 75)), 2),
        "percentile_90": round(float(np.percentile(daily, 90)), 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# AGGREGATED DASHBOARD DATA
# ──────────────────────────────────────────────────────────────────────────────

def get_all_analytics() -> dict:
    """Single call used by Flask route."""
    df = load_dataframe()
    return {
        "summary":            get_summary(df),
        "daily_trend":        get_daily_trend(df, 30),
        "weekly_trend":       get_weekly_trend(df, 12),
        "monthly_trend":      get_monthly_trend(df, 12),
        "yearly_trend":       get_yearly_trend(df),
        "platform_dist":      get_platform_distribution(df),
        "keyword_dist":       get_keyword_distribution(df),
        "hourly_dist":        get_hourly_distribution(df),
        "keyword_trends":     get_keyword_trends_over_time(df, "daily"),
        "heatmap":            get_platform_keyword_heatmap(df),
        "recent_articles":    get_recent_articles(df, 100),
        "numpy_stats":        get_numpy_stats(df),
    }