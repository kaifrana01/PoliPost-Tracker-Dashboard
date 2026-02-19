"""
seed_demo.py
============
Generates realistic demo data so you can explore the dashboard
immediately without waiting for RSS fetches.

Run ONCE before starting the app:
    python seed_demo.py
"""

import random
import hashlib
from datetime import datetime, timedelta

from database import init_db, insert_many_articles

KEYWORDS = [
    "politics modi",
    "pm modi",
    "rahul gandhi",
    "Parliament",
    "Policy",
    "Govt",
    "Human Rights",
]

PLATFORMS = [
    "Google News",
    "Yahoo News",
    "Bing News",
    "DuckDuckGo",
    "Opera News",
    "Reuters",
]

SOURCES = {
    "Google News": ["NDTV", "India Today", "The Hindu", "Hindustan Times"],
    "Yahoo News":  ["Economic Times", "LiveMint", "Financial Express"],
    "Bing News":   ["Times of India", "India Today", "Business Standard"],
    "DuckDuckGo":  ["The Wire", "Scroll.in", "News18"],
    "Opera News":  ["Deccan Herald", "Tribune India", "Indian Express"],
    "Reuters":     ["Reuters India", "AP News", "AFP"],
}

TITLE_TEMPLATES = {
    "pm modi":       [
        "PM Modi meets {person} to discuss bilateral ties",
        "Modi government unveils new {scheme} initiative",
        "Modi addresses nation on {topic}",
        "PM Modi inaugurates {project} in {state}",
        "Modi's economic agenda: what experts say",
    ],
    "rahul gandhi":  [
        "Rahul Gandhi slams government over {topic}",
        "Congress leader Rahul Gandhi visits {state}",
        "Rahul Gandhi holds rally in {city}",
        "Rahul Gandhi demands inquiry into {issue}",
    ],
    "Parliament":    [
        "Parliament session: {bill} tabled in Lok Sabha",
        "Opposition walkout over {topic} in Parliament",
        "Parliament passes {bill} amid debate",
        "Winter session of Parliament: key highlights",
    ],
    "Policy":        [
        "Government announces new {sector} policy",
        "New education policy to be rolled out in {state}",
        "Policy changes in {sector} — impact analysis",
        "Centre revises {policy} norms",
    ],
    "Govt":          [
        "Govt raises infra spending by {pct}%",
        "Central govt launches {scheme} for rural areas",
        "Govt sets 2024 target for {project}",
    ],
    "Human Rights":  [
        "Human rights activists protest in {city}",
        "UN report highlights human rights issues in India",
        "Courts rule on fundamental rights in landmark case",
        "Human rights groups demand action on {issue}",
    ],
    "politics modi": [
        "Modi's BJP dominates state polls in {state}",
        "Political analysis: Modi's approval rating soars",
        "BJP strategy for 2024 elections: insiders reveal",
    ],
}

FILL = {
    "person":  ["US President", "PM of Japan", "EU leaders", "UAE President"],
    "scheme":  ["PM Kisan", "Jan Dhan", "Digital India", "Swachh Bharat"],
    "topic":   ["economy", "defence", "agriculture", "inflation"],
    "project": ["expressway", "metro rail", "power plant", "sea bridge"],
    "state":   ["Maharashtra", "Gujarat", "UP", "Rajasthan", "Tamil Nadu"],
    "city":    ["Mumbai", "Delhi", "Chennai", "Kolkata", "Hyderabad"],
    "bill":    ["Data Protection Bill", "CAA amendments", "Farm Bill", "Banking Reform Bill"],
    "sector":  ["education", "health", "defence", "energy", "agriculture"],
    "policy":  ["GST", "FDI", "NEET", "banking"],
    "pct":     ["12", "18", "25", "30"],
    "issue":   ["minority rights", "press freedom", "internet shutdown", "farmers"],
}


def random_fill(template: str) -> str:
    import re
    for key, vals in FILL.items():
        if f'{{{key}}}' in template:
            template = template.replace(f'{{{key}}}', random.choice(vals))
    return template


def make_url(title: str, platform: str, idx: int) -> str:
    h = hashlib.md5(f"{title}{platform}{idx}".encode()).hexdigest()[:10]
    domain = platform.lower().replace(" ", "")
    return f"https://{domain}.example.com/article/{h}"


def generate_articles(n_days=120, articles_per_day_range=(5, 30)):
    articles = []
    base_date = datetime.utcnow() - timedelta(days=n_days)

    for day in range(n_days):
        current_date = base_date + timedelta(days=day)
        # simulate more articles on weekdays / trending periods
        n_today = random.randint(*articles_per_day_range)
        if current_date.weekday() < 5:
            n_today += random.randint(0, 10)

        for _ in range(n_today):
            keyword  = random.choice(KEYWORDS)
            platform = random.choice(PLATFORMS)
            source   = random.choice(SOURCES[platform])

            # find a title template matching keyword
            kw_lower = keyword.lower()
            matched_key = None
            for k in TITLE_TEMPLATES:
                if k in kw_lower or kw_lower in k:
                    matched_key = k
                    break
            if matched_key is None:
                matched_key = random.choice(list(TITLE_TEMPLATES.keys()))

            template = random.choice(TITLE_TEMPLATES[matched_key])
            title    = random_fill(template)

            hour   = random.choices(range(24), weights=[
                1,1,1,1,2,3,5,8,10,12,12,10,
                10,11,10,9,9,8,8,7,6,5,3,2
            ])[0]
            minute = random.randint(0, 59)
            pub_dt = current_date.replace(hour=hour, minute=minute, second=random.randint(0,59))

            articles.append({
                "title":        title,
                "description":  f"Full coverage of {title.lower()} — latest updates.",
                "url":          make_url(title, platform, len(articles)),
                "source_name":  source,
                "platform":     platform,
                "keyword":      keyword,
                "published_at": pub_dt.isoformat(),
                "author":       random.choice(["Staff Reporter", "PTI", "ANI", "Special Correspondent", ""]),
            })

    return articles


if __name__ == "__main__":
    print("Initialising database …")
    init_db()

    print("Generating demo articles …")
    arts = generate_articles(n_days=120)
    inserted = 0
    for a in arts:
        from database import insert_article
        inserted += insert_article(a)

    print(f"Done! Inserted {inserted} demo articles (out of {len(arts)} generated).")
    print("\nNow run:  python app.py")