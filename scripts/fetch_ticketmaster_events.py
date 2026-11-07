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
fetch_ticketmaster_events.py
----------------------------
Pulls upcoming live events (concerts, sports, theater, family) within a 50-mile
radius of Dana Point from the Ticketmaster Discovery API.

Why this matters for PULSE:
  Events are the #1 forward-looking demand signal. A sold-out Honda Center show
  or Angel Stadium series spills lodging demand into Orange County coastal
  markets. Knowing the 90-day event landscape lets the DMO anticipate
  compression, time campaigns, and brief partner hotels.

FREE API KEY — instant signup:
  https://developer.ticketmaster.com/

Set env var: TICKETMASTER_API_KEY=your_key_here
Or add to .env file at project root.

Quota: 5,000 calls/day, 5 req/sec. We make at most ~10 calls/run.

Tables written:
  ticketmaster_events — event_id, name, event_date, venue_name, venue_city,
                        venue_lat, venue_lng, distance_miles, segment, genre,
                        price_min, price_max, status, url, updated_at

Cross-relationships (see build_table_relationships.py):
  • ticketmaster_events × vdp_events    → cross_ref on event_date (regional vs local)
  • ticketmaster_events × kpi_daily_summary → context on as_of_date (event-driven occupancy)
  • ticketmaster_events × datafy_overview_dma → cross_ref (feeder market overlap)
  • ticketmaster_events × insights_daily → derived_from (event ROI / compression outlook)
"""

import os
import sys
import sqlite3
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

load_dotenv(ROOT / ".env")

TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY", "")
TM_BASE = "https://app.ticketmaster.com/discovery/v2/events.json"

# Dana Point coords + 50-mile radius captures Honda Center, Angel Stadium,
# OC Fair, Pacific Amphitheatre, Irvine Meadows, Long Beach, San Diego suburbs.
DANA_POINT_LAT = 33.4669
DANA_POINT_LNG = -117.6981
SEARCH_RADIUS_MILES = 50
WINDOW_DAYS_FORWARD = 90

INIT_SQL = """
CREATE TABLE IF NOT EXISTS ticketmaster_events (
    event_id        TEXT NOT NULL PRIMARY KEY,
    name            TEXT,
    event_date      TEXT,
    event_time      TEXT,
    venue_name      TEXT,
    venue_city      TEXT,
    venue_state     TEXT,
    venue_lat       REAL,
    venue_lng       REAL,
    distance_miles  REAL,
    segment         TEXT,
    genre           TEXT,
    sub_genre       TEXT,
    price_min       REAL,
    price_max       REAL,
    currency        TEXT,
    status          TEXT,
    url             TEXT,
    image_url       TEXT,
    updated_at      TEXT DEFAULT (datetime('now'))
);
"""

# Demo seed rows used when no API key is configured. Keeps downstream charts
# rendering and lets reviewers see the table structure without signing up.
_DEMO_ROWS = [
    ("DEMO-OHANA-2026",        "Ohana Festival 2026",
     "2026-09-25", "16:00",   "Doheny State Beach",  "Dana Point",   "CA",
     33.4625, -117.6877,    0.5, "Music",   "Rock",      "Festival",   195.0, 1200.0, "USD",
     "onsale",  "https://example.com/ohana", None),
    ("DEMO-ANGELS-2026",       "LA Angels vs Yankees",
     "2026-05-18", "19:07",   "Angel Stadium",       "Anaheim",      "CA",
     33.8003, -117.8827,   28.0, "Sports",  "Baseball",  "MLB",         28.0,  450.0, "USD",
     "onsale",  "https://example.com/angels", None),
    ("DEMO-DUCKS-2026",        "Anaheim Ducks Playoff Game",
     "2026-05-12", "19:00",   "Honda Center",        "Anaheim",      "CA",
     33.8078, -117.8765,   28.5, "Sports",  "Hockey",    "NHL",         85.0,  900.0, "USD",
     "onsale",  "https://example.com/ducks", None),
    ("DEMO-SOCCER-2026",       "OC Soccer Match",
     "2026-06-05", "19:30",   "Championship Stadium","Irvine",       "CA",
     33.6846, -117.8265,   18.0, "Sports",  "Soccer",    "MLS",         22.0,  150.0, "USD",
     "onsale",  "https://example.com/soccer", None),
    ("DEMO-PACAMP-2026",       "Country Concert at Pacific Amphitheatre",
     "2026-07-22", "19:30",   "Pacific Amphitheatre","Costa Mesa",   "CA",
     33.6789, -117.9111,   16.0, "Music",   "Country",   "Arena",       65.0,  295.0, "USD",
     "onsale",  "https://example.com/pacamp", None),
    ("DEMO-OCFAIR-2026",       "OC Fair Concert Series",
     "2026-07-30", "20:00",   "OC Fair & Event Center","Costa Mesa", "CA",
     33.6789, -117.9111,   16.0, "Music",   "Pop",       "Festival",    14.0,  120.0, "USD",
     "onsale",  "https://example.com/ocfair", None),
    ("DEMO-COMEDY-2026",       "Comedy Night at City National Grove",
     "2026-06-14", "20:00",   "City National Grove", "Anaheim",      "CA",
     33.8093, -117.8754,   28.5, "Arts",    "Comedy",    "Stand-Up",    45.0,  175.0, "USD",
     "onsale",  "https://example.com/comedy", None),
    ("DEMO-BROADWAY-2026",     "Broadway Tour: Hamilton",
     "2026-08-03", "19:30",   "Segerstrom Center",   "Costa Mesa",   "CA",
     33.6905, -117.8769,   17.5, "Arts",    "Theatre",   "Musical",     95.0,  385.0, "USD",
     "onsale",  "https://example.com/segerstrom", None),
]


def _fetch_page(api_key: str, page: int, start_date: str, end_date: str) -> dict:
    """Fetch one page of events from the Discovery API."""
    params = {
        "apikey":       api_key,
        "latlong":      f"{DANA_POINT_LAT},{DANA_POINT_LNG}",
        "radius":       str(SEARCH_RADIUS_MILES),
        "unit":         "miles",
        "startDateTime": f"{start_date}T00:00:00Z",
        "endDateTime":   f"{end_date}T23:59:59Z",
        "size":         "100",
        "page":         str(page),
        "sort":         "date,asc",
        "locale":       "*",
    }
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(TM_BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  WARN: Ticketmaster page {page} attempt {attempt+1} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise last_exc


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance between two lat/lng pairs in miles."""
    from math import radians, sin, cos, asin, sqrt
    rlat1, rlat2 = radians(lat1), radians(lat2)
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(rlat1) * cos(rlat2) * sin(dlng / 2) ** 2
    return round(2 * 3958.8 * asin(sqrt(a)), 2)


def _normalize_event(ev: dict) -> tuple | None:
    """Flatten a Discovery API event dict into a row tuple matching INIT_SQL."""
    try:
        event_id = ev.get("id")
        name = ev.get("name", "")
        url = ev.get("url", "")
        status = (ev.get("dates", {}).get("status", {}).get("code") or "").lower()

        dates = ev.get("dates", {}).get("start", {})
        event_date = dates.get("localDate")
        event_time = dates.get("localTime")

        # Classification (segment > genre > subGenre)
        cls = (ev.get("classifications") or [{}])[0]
        segment   = (cls.get("segment")   or {}).get("name")
        genre     = (cls.get("genre")     or {}).get("name")
        sub_genre = (cls.get("subGenre")  or {}).get("name")

        # Venue
        venue = (ev.get("_embedded", {}).get("venues") or [{}])[0]
        venue_name  = venue.get("name")
        venue_city  = (venue.get("city")  or {}).get("name")
        venue_state = (venue.get("state") or {}).get("stateCode")
        loc = venue.get("location") or {}
        venue_lat = float(loc["latitude"])  if loc.get("latitude")  else None
        venue_lng = float(loc["longitude"]) if loc.get("longitude") else None
        distance = (
            _haversine_miles(DANA_POINT_LAT, DANA_POINT_LNG, venue_lat, venue_lng)
            if venue_lat is not None and venue_lng is not None else None
        )

        # Price ranges (take the first/widest span)
        price_min = price_max = currency = None
        prices = ev.get("priceRanges") or []
        if prices:
            price_min = prices[0].get("min")
            price_max = prices[0].get("max")
            currency  = prices[0].get("currency")

        # Image: pick the largest 16x9
        image_url = None
        for img in ev.get("images", []):
            if img.get("ratio") == "16_9" and img.get("width", 0) >= 640:
                image_url = img.get("url")
                break

        return (
            event_id, name, event_date, event_time,
            venue_name, venue_city, venue_state, venue_lat, venue_lng, distance,
            segment, genre, sub_genre,
            price_min, price_max, currency,
            status, url, image_url,
        )
    except Exception as exc:
        print(f"  WARN: skip malformed event ({exc})")
        return None


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    if not TICKETMASTER_API_KEY:
        print(
            "[SKIP] fetch_ticketmaster_events.py — TICKETMASTER_API_KEY not set.\n"
            "       Get a free key instantly: https://developer.ticketmaster.com/\n"
            "       Add  TICKETMASTER_API_KEY=your_key  to your .env file.\n"
            "       Seeding 8 demo events for chart rendering."
        )
        for row in _DEMO_ROWS:
            conn.execute(
                """INSERT OR REPLACE INTO ticketmaster_events
                   (event_id, name, event_date, event_time,
                    venue_name, venue_city, venue_state, venue_lat, venue_lng, distance_miles,
                    segment, genre, sub_genre,
                    price_min, price_max, currency,
                    status, url, image_url)
                   VALUES (?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?,
                           ?, ?, ?,
                           ?, ?, ?,
                           ?, ?, ?)""",
                row,
            )
        conn.commit()
        conn.close()
        return len(_DEMO_ROWS)

    today    = datetime.now().date()
    end_date = today + timedelta(days=WINDOW_DAYS_FORWARD)
    start_s, end_s = today.isoformat(), end_date.isoformat()

    inserted = 0
    page     = 0
    pages_seen = 0
    while True:
        try:
            data = _fetch_page(TICKETMASTER_API_KEY, page, start_s, end_s)
        except Exception as exc:
            print(f"[WARN] Ticketmaster page {page} failed: {exc}")
            break

        events = (data.get("_embedded") or {}).get("events") or []
        if not events:
            break

        rows = [_normalize_event(e) for e in events]
        rows = [r for r in rows if r is not None]

        conn.executemany(
            """INSERT OR REPLACE INTO ticketmaster_events
               (event_id, name, event_date, event_time,
                venue_name, venue_city, venue_state, venue_lat, venue_lng, distance_miles,
                segment, genre, sub_genre,
                price_min, price_max, currency,
                status, url, image_url)
               VALUES (?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?)""",
            rows,
        )
        conn.commit()
        inserted += len(rows)
        pages_seen += 1
        print(f"[OK]   Ticketmaster page {page}: {len(rows)} events")

        page_info = data.get("page", {})
        total_pages = page_info.get("totalPages", 1)
        if page >= total_pages - 1 or pages_seen >= 10:
            break
        page += 1
        time.sleep(0.25)  # respect 5 req/sec ceiling

    conn.close()
    return inserted


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_ticketmaster_events: {n} rows inserted/updated")
    sys.exit(0)
