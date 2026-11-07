# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
fetch_wikipedia_pageviews.py
----------------------------
Pulls daily Wikipedia pageviews for Dana Point destination articles from the
Wikimedia Pageviews API (no key, 100 req/sec rate limit).

Why this matters for PULSE:
  Wikipedia pageviews are a leading awareness/intent signal. A spike in
  "Dana Point, California" pageviews 4–6 weeks ahead of a peak weekend
  predicts a corresponding lift in occupancy and Datafy visitor days.
  Pairs with google_trends_weekly to triangulate organic interest.

NO API KEY required.
Docs: https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/

Tables written:
  wikipedia_pageviews_daily — as_of_date, article_title, project,
                              access, agent, views, updated_at

Articles tracked (Dana Point destination cluster):
  • Dana_Point,_California
  • Doheny_State_Beach
  • Mission_San_Juan_Capistrano
  • Salt_Creek_Beach
  • Dana_Point_Harbor
  • Laguna_Beach,_California   (adjacent / competitive set)
  • San_Clemente,_California   (adjacent / competitive set)

Cross-relationships:
  • wikipedia_pageviews_daily × google_trends_weekly       → cross_platform on time_period
  • wikipedia_pageviews_daily × kpi_daily_summary          → context on as_of_date
  • wikipedia_pageviews_daily × datafy_attribution_website_kpis → cross_ref (intent vs. visits)
  • wikipedia_pageviews_daily × insights_daily             → derived_from
"""

import sys
import sqlite3
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

WIKI_BASE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
    "en.wikipedia.org/all-access/all-agents/{article}/daily/{start}/{end}"
)

ARTICLES = [
    "Dana_Point,_California",
    "Doheny_State_Beach",
    "Mission_San_Juan_Capistrano",
    "Salt_Creek_Beach",
    "Dana_Point_Harbor",
    "Laguna_Beach,_California",
    "San_Clemente,_California",
]

# Pull a 1-year window so YOY analysis is possible from day 1.
LOOKBACK_DAYS = 365

INIT_SQL = """
CREATE TABLE IF NOT EXISTS wikipedia_pageviews_daily (
    as_of_date     TEXT NOT NULL,
    article_title  TEXT NOT NULL,
    project        TEXT,
    access         TEXT,
    agent          TEXT,
    views          INTEGER,
    updated_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of_date, article_title) ON CONFLICT REPLACE
);
"""

HEADERS = {
    # Wikimedia requires a descriptive User-Agent for analytics endpoints.
    "User-Agent": "VDP-PULSE-Analytics/1.0 (https://github.com/gloconllc/vdp_dashboard)",
    "Accept": "application/json",
}


def _fetch_article(article: str, start: str, end: str) -> list[dict]:
    """Fetch daily pageviews for one article. Returns [] on 404 (article missing)."""
    url = WIKI_BASE.format(article=quote(article, safe=",_"), start=start, end=end)
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 404:
                print(f"  WARN: Wikipedia article '{article}' not found (404)")
                return []
            r.raise_for_status()
            return r.json().get("items", [])
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  WARN: Wikipedia '{article}' attempt {attempt+1} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise last_exc


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    end   = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)
    # Wikimedia API expects YYYYMMDD with no separator
    start_s = start.strftime("%Y%m%d")
    end_s   = end.strftime("%Y%m%d")

    total = 0
    for article in ARTICLES:
        try:
            items = _fetch_article(article, start_s, end_s)
            if not items:
                continue
            rows = []
            for item in items:
                ts = item.get("timestamp", "")
                # timestamp format: YYYYMMDDHH → take YYYY-MM-DD
                if len(ts) < 8:
                    continue
                as_of_date = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
                rows.append((
                    as_of_date,
                    item.get("article", article),
                    item.get("project", "en.wikipedia"),
                    item.get("access", "all-access"),
                    item.get("agent", "all-agents"),
                    int(item.get("views") or 0),
                ))
            conn.executemany(
                """INSERT OR REPLACE INTO wikipedia_pageviews_daily
                   (as_of_date, article_title, project, access, agent, views)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows,
            )
            conn.commit()
            total += len(rows)
            print(f"[OK]   Wikipedia '{article}': {len(rows)} daily observations")
        except Exception as exc:
            print(f"[WARN] Wikipedia '{article}' failed: {exc}")
        time.sleep(0.1)  # well under 100 req/s ceiling, polite

    conn.close()
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_wikipedia_pageviews: {n} rows inserted/updated")
    sys.exit(0)
