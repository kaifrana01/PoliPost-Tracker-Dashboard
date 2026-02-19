"""
app.py
======
Main Flask application for the News Intelligence Dashboard.

Run:
    python app.py
    
Then visit: http://127.0.0.1:5000
"""

import threading
import schedule
import time
from flask import Flask, render_template, jsonify, request

from database import init_db
from analytics import get_all_analytics
from data_fetcher import fetch_all, KEYWORDS, PLATFORMS

app = Flask(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    """Render the main dashboard HTML."""
    return render_template("dashboard.html",
                           keywords=KEYWORDS,
                           platforms=list(PLATFORMS.keys()))


@app.route("/api/analytics")
def api_analytics():
    """Return all analytics data as JSON."""
    data = get_all_analytics()
    return jsonify(data)


@app.route("/api/fetch", methods=["POST"])
def api_fetch():
    """Manually trigger a data fetch."""
    def _run():
        fetch_all()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "Data fetch started in background."})


@app.route("/api/keywords")
def api_keywords():
    return jsonify({"keywords": KEYWORDS})


@app.route("/api/platforms")
def api_platforms():
    return jsonify({"platforms": list(PLATFORMS.keys())})


# ──────────────────────────────────────────────────────────────────────────────
# BACKGROUND SCHEDULER  – auto-fetch every 6 hours
# ──────────────────────────────────────────────────────────────────────────────

def _scheduled_fetch():
    print("[SCHEDULER] Starting scheduled fetch …")
    fetch_all()
    print("[SCHEDULER] Done.")


def start_scheduler():
    schedule.every(6).hours.do(_scheduled_fetch)

    def _run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)

    t = threading.Thread(target=_run_scheduler, daemon=True)
    t.start()


# ──────────────────────────────────────────────────────────────────────────────
# STARTUP
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()

    # Initial fetch on startup (in background so server starts instantly)
    t = threading.Thread(target=fetch_all, daemon=True)
    t.start()

    # Start the background scheduler
    start_scheduler()

    print("\n" + "="*60)
    print("  News Intelligence Dashboard")
    print("  Open: http://127.0.0.1:5000")
    print("="*60 + "\n")
    app.run(debug=True)