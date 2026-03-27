"""
fetch_google_trends.py
----------------------
Pulls Google Trends search interest data — the LEADING INDICATOR for future
hotel demand used by platforms like Arrivalist, Placer.ai, and DMO research
tools. Search intent typically leads bookings by 2–6 weeks, making it the
earliest publicly available demand signal.

Requires: pip install pytrends
No API key needed. Rate-limited by Google to ~1 request every 5 seconds.

Terms tracked:
  Primary     — Dana Point branded search terms
  Event       — Ohana Fest and major event searches
  Competitor  — Comparable coastal OC destinations
  Regional    — Orange County / SoCal travel intent

Table written:
  google_trends_weekly — term, market, category, week_date, interest_idx, geo
"""

import sys
import time
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

# Search terms grouped by analytical purpose
# (term, market_label, category)
SEARCH_TERMS = [
    # Dana Point — primary destination intent
    ("dana point hotel",      "Dana Point",  "primary"),
    ("dana point beach",      "Dana Point",  "primary"),
    ("visit dana point",      "Dana Point",  "primary"),
    ("doheny state beach",    "Dana Point",  "primary"),
    # Events — seasonal demand spikes
    ("ohana fest",            "Dana Point",  "event"),
    ("dana point whale watch","Dana Point",  "event"),
    # Competitors — share-of-search benchmarking
    ("laguna beach hotel",    "Competitor",  "competitor"),
    ("san clemente hotel",    "Competitor",  "competitor"),
    ("huntington beach hotel","Competitor",  "competitor"),
    # Regional travel intent
    ("orange county hotel",   "Regional",   "regional"),
    ("socal beach vacation",  "Regional",   "regional"),
]

INIT_SQL = """
CREATE TABLE IF NOT EXISTS google_trends_weekly (
    term         TEXT    NOT NULL,
    market       TEXT,
    category     TEXT,
    week_date    TEXT    NOT NULL,
    interest_idx INTEGER,
    geo          TEXT    DEFAULT 'US-CA',
    updated_at   TEXT    DEFAULT (datetime('now')),
    UNIQUE(term, week_date) ON CONFLICT REPLACE
);
"""

GEO       = "US-CA"
TIMEFRAME = "today 12-m"   # last 12 months of weekly data
BATCH_SZ  = 5              # Google Trends max per payload


def _build_pytrends():
    try:
        from pytrends.request import TrendReq
        # Note: avoid retries/backoff_factor — incompatible with urllib3 >= 2.0
        return TrendReq(hl="en-US", tz=420, timeout=(10, 30))
    except ImportError:
        return None


def _pull_batch(pytrends, terms: list[str], conn: sqlite3.Connection) -> int:
    """Pull one batch (≤5 terms) and upsert into DB. Returns rows inserted."""
    try:
        pytrends.build_payload(terms, timeframe=TIMEFRAME, geo=GEO)
        df = pytrends.interest_over_time()
    except Exception as exc:
        print(f"[WARN] Google Trends batch {terms}: {exc}")
        return 0

    if df is None or df.empty:
        return 0

    df = df.drop(columns=["isPartial"], errors="ignore")
    inserted = 0
    for term in terms:
        if term not in df.columns:
            continue
        term_meta = next((t for t in SEARCH_TERMS if t[0] == term), None)
        market, category = (term_meta[1], term_meta[2]) if term_meta else ("Other", "other")

        rows = [
            (term, market, category, str(idx.date()), int(val), GEO)
            for idx, val in df[term].items()
            if val is not None
        ]
        conn.executemany(
            """INSERT OR REPLACE INTO google_trends_weekly
               (term, market, category, week_date, interest_idx, geo)
               VALUES (?, ?, ?, ?, ?, ?)""",
            rows,
        )
        inserted += len(rows)
        print(f"[OK]   Google Trends '{term}': {len(rows)} weeks")

    conn.commit()
    return inserted


def main() -> int:
    pytrends = _build_pytrends()
    if pytrends is None:
        print(
            "[SKIP] fetch_google_trends.py — pytrends not installed.\n"
            "       Run:  pip install pytrends  (already in requirements.txt)\n"
            "       Or:   pip install -r requirements.txt"
        )
        return 0

    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    all_terms = [t[0] for t in SEARCH_TERMS]
    total     = 0

    # Process in batches of BATCH_SZ with delay between requests
    for i in range(0, len(all_terms), BATCH_SZ):
        batch   = all_terms[i : i + BATCH_SZ]
        inserted = _pull_batch(pytrends, batch, conn)
        total   += inserted
        if i + BATCH_SZ < len(all_terms):
            time.sleep(3)   # respect rate limit between batches

    conn.close()
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_google_trends: {n} rows inserted/updated")
    sys.exit(0)
