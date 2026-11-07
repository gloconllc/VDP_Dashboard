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
fetch_airnow_aqi.py
-------------------
Pulls daily Air Quality Index (AQI) for Dana Point and the South Coast OC
ZIP cluster from EPA AirNow.

Why this matters for PULSE:
  Air quality is a direct visitor-experience and event-impact lever:
    • Beach attendance drops sharply on AQI > 100 (Unhealthy for Sensitive Groups)
    • Outdoor events (Ohana Fest, Concours d'Elegance, Festival of Whales,
      Doheny Blues) face cancellation/refund risk on AQI > 150
    • Wildfire smoke (June–November) is the dominant downside risk for OC
  Pairing AQI with kpi_daily_summary surfaces "soft demand" days where the
  weather LOOKS good but the air keeps drive-market visitors away.

FREE API KEY — instant:
  https://docs.airnowapi.org/

Set env var: AIRNOW_API_KEY=your_key_here
Or add to .env file at project root.

Tables written:
  airnow_aqi_daily — as_of_date, zip_code, city, parameter, aqi, category_num,
                     category_name, latitude, longitude, updated_at

ZIPs tracked:
  92629  Dana Point
  92651  Laguna Beach
  92675  San Juan Capistrano
  92672  San Clemente
  92660  Newport Beach

Cross-relationships:
  • airnow_aqi_daily × kpi_daily_summary  → context on as_of_date (visitor experience)
  • airnow_aqi_daily × vdp_events         → context on event_date (outdoor cancellation risk)
  • airnow_aqi_daily × ticketmaster_events → context on event_date
  • airnow_aqi_daily × weather_monthly    → cross_ref on month
"""

import os
import sys
import sqlite3
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

load_dotenv(ROOT / ".env")

AIRNOW_API_KEY = os.getenv("AIRNOW_API_KEY", "")
AIRNOW_BASE    = "https://www.airnowapi.org/aq/observation/zipCode/historical/"

# (zip, city) tuples for the South OC visitor corridor
ZIPS = [
    ("92629", "Dana Point"),
    ("92651", "Laguna Beach"),
    ("92675", "San Juan Capistrano"),
    ("92672", "San Clemente"),
    ("92660", "Newport Beach"),
]

# Pull last 30 days; AirNow historical endpoint requires per-day calls.
LOOKBACK_DAYS = 30

INIT_SQL = """
CREATE TABLE IF NOT EXISTS airnow_aqi_daily (
    as_of_date     TEXT NOT NULL,
    zip_code       TEXT NOT NULL,
    city           TEXT,
    parameter      TEXT NOT NULL,
    aqi            INTEGER,
    category_num   INTEGER,
    category_name  TEXT,
    latitude       REAL,
    longitude      REAL,
    updated_at     TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of_date, zip_code, parameter) ON CONFLICT REPLACE
);
"""

# Demo data — typical OC summer baselines, used when no API key is configured.
_DEMO_AQI_VALUES = [
    # (offset_days, zip, city, param, aqi, cat_num, cat_name)
    (1, "92629", "Dana Point",          "PM2.5", 38, 1, "Good"),
    (1, "92629", "Dana Point",          "OZONE", 52, 2, "Moderate"),
    (1, "92651", "Laguna Beach",        "PM2.5", 41, 1, "Good"),
    (1, "92651", "Laguna Beach",        "OZONE", 56, 2, "Moderate"),
    (1, "92675", "San Juan Capistrano", "PM2.5", 44, 1, "Good"),
    (1, "92675", "San Juan Capistrano", "OZONE", 61, 2, "Moderate"),
    (1, "92672", "San Clemente",        "PM2.5", 36, 1, "Good"),
    (1, "92672", "San Clemente",        "OZONE", 49, 1, "Good"),
    (1, "92660", "Newport Beach",       "PM2.5", 35, 1, "Good"),
    (1, "92660", "Newport Beach",       "OZONE", 47, 1, "Good"),
    (7, "92629", "Dana Point",          "PM2.5", 42, 1, "Good"),
    (7, "92629", "Dana Point",          "OZONE", 58, 2, "Moderate"),
    (14, "92629", "Dana Point",         "PM2.5", 39, 1, "Good"),
    (14, "92629", "Dana Point",         "OZONE", 54, 2, "Moderate"),
    (21, "92629", "Dana Point",         "PM2.5", 45, 1, "Good"),
    (21, "92629", "Dana Point",         "OZONE", 63, 2, "Moderate"),
    (28, "92629", "Dana Point",         "PM2.5", 40, 1, "Good"),
    (28, "92629", "Dana Point",         "OZONE", 51, 2, "Moderate"),
]


def _fetch_zip_day(zip_code: str, when: date) -> list[dict]:
    """Fetch AirNow historical observations for one ZIP on one date."""
    params = {
        "format":     "application/json",
        "zipCode":    zip_code,
        "date":       when.strftime("%Y-%m-%dT00-0000"),
        "distance":   "25",
        "API_KEY":    AIRNOW_API_KEY,
    }
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(AIRNOW_BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json() or []
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  WARN: AirNow {zip_code}/{when} attempt {attempt+1} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise last_exc


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    if not AIRNOW_API_KEY:
        print(
            "[SKIP] fetch_airnow_aqi.py — AIRNOW_API_KEY not set.\n"
            "       Get a free key instantly: https://docs.airnowapi.org/\n"
            "       Add  AIRNOW_API_KEY=your_key  to your .env file.\n"
            "       Seeding demo AQI values for South OC ZIPs."
        )
        today = date.today()
        rows = []
        for offset, z, city, param, aqi, cat, cat_name in _DEMO_AQI_VALUES:
            d = (today - timedelta(days=offset)).isoformat()
            rows.append((d, z, city, param, aqi, cat, cat_name, None, None))
        conn.executemany(
            """INSERT OR REPLACE INTO airnow_aqi_daily
               (as_of_date, zip_code, city, parameter, aqi,
                category_num, category_name, latitude, longitude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        conn.close()
        return len(rows)

    today = date.today()
    total = 0
    rows  = []
    for offset in range(1, LOOKBACK_DAYS + 1):
        when = today - timedelta(days=offset)
        for zip_code, city in ZIPS:
            try:
                obs = _fetch_zip_day(zip_code, when)
                for o in obs:
                    rows.append((
                        o.get("DateObserved", "").strip() or when.isoformat(),
                        zip_code,
                        city,
                        o.get("ParameterName"),
                        int(o.get("AQI")) if o.get("AQI") is not None else None,
                        (o.get("Category") or {}).get("Number"),
                        (o.get("Category") or {}).get("Name"),
                        o.get("Latitude"),
                        o.get("Longitude"),
                    ))
            except Exception as exc:
                print(f"[WARN] AirNow {zip_code}/{when} failed: {exc}")
            time.sleep(0.1)  # gentle on the API

    if rows:
        conn.executemany(
            """INSERT OR REPLACE INTO airnow_aqi_daily
               (as_of_date, zip_code, city, parameter, aqi,
                category_num, category_name, latitude, longitude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        total = len(rows)
        print(f"[OK]   AirNow AQI: {total} observations across {len(ZIPS)} ZIPs")
    else:
        print("[WARN] AirNow returned no rows.")

    conn.close()
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_airnow_aqi: {n} rows inserted/updated")
    sys.exit(0)
