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
fetch_noaa_tides.py
-------------------
Pulls daily tide predictions and observed water temperature for Dana Point
Harbor from NOAA CO-OPS (Tides & Currents).

Why this matters for PULSE:
  Dana Point Harbor is a primary visitor draw — whale watching, sportfishing,
  sailing schools, harbor walks, dock dining. Tide cycles directly affect:
    • Whale-watching boat departures (low-tide groundings)
    • Beach width at Doheny / Salt Creek (visitor capacity)
    • Tidepool tourism (negative low tides = peak interest)
    • Boat-based fishing/charter scheduling
  Forward-looking tide data lets the DMO time messaging and partner ops.

NO API KEY required.
Docs: https://api.tidesandcurrents.noaa.gov/api/prod/
Station 9410660 (Los Angeles) is the official reference station for Dana
Point Harbor tide predictions per the City's coastal hazards memorandum.

Tables written:
  noaa_tides_daily — as_of_date, station_id, station_name,
                     hi_tide_1_time, hi_tide_1_height,
                     lo_tide_1_time, lo_tide_1_height,
                     hi_tide_2_time, hi_tide_2_height,
                     lo_tide_2_time, lo_tide_2_height,
                     tide_range_ft, has_negative_low,
                     water_temp_f_avg, updated_at

Cross-relationships:
  • noaa_tides_daily × kpi_daily_summary  → context on as_of_date (visitor capacity)
  • noaa_tides_daily × vdp_events         → context on event_date (harbor events)
  • noaa_tides_daily × noaa_marine_monthly → enriches (daily detail vs monthly)
  • noaa_tides_daily × ticketmaster_events → context on event_date (boat-based events)
"""

import sys
import sqlite3
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

NOAA_BASE   = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
STATION_ID  = "9410660"   # Los Angeles — reference station for Dana Point
STATION_NAME = "Los Angeles (Dana Point reference)"

# Pull 30 days back + 60 days forward (forward-looking event planning)
DAYS_BACK    = 30
DAYS_FORWARD = 60

INIT_SQL = """
CREATE TABLE IF NOT EXISTS noaa_tides_daily (
    as_of_date        TEXT NOT NULL,
    station_id        TEXT NOT NULL,
    station_name      TEXT,
    hi_tide_1_time    TEXT,
    hi_tide_1_height  REAL,
    lo_tide_1_time    TEXT,
    lo_tide_1_height  REAL,
    hi_tide_2_time    TEXT,
    hi_tide_2_height  REAL,
    lo_tide_2_time    TEXT,
    lo_tide_2_height  REAL,
    tide_range_ft     REAL,
    has_negative_low  INTEGER,
    water_temp_f_avg  REAL,
    updated_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(as_of_date, station_id) ON CONFLICT REPLACE
);
"""


def _fetch_noaa(product: str, start: date, end: date) -> list[dict]:
    """Generic NOAA CO-OPS fetcher returning JSON 'data' or 'predictions' list."""
    params = {
        "product":    product,
        "application": "VDP-PULSE",
        "begin_date": start.strftime("%Y%m%d"),
        "end_date":   end.strftime("%Y%m%d"),
        "datum":      "MLLW",
        "station":    STATION_ID,
        "time_zone":  "lst_ldt",
        "units":      "english",
        "interval":   "hilo" if product == "predictions" else "h",
        "format":     "json",
    }
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(NOAA_BASE, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data.get("predictions") or data.get("data") or []
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  WARN: NOAA {product} attempt {attempt+1} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise last_exc


def _aggregate_tides(predictions: list[dict]) -> dict[str, dict]:
    """Group hi/lo tide predictions by date into one row per day."""
    by_date: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    for p in predictions:
        # t = "YYYY-MM-DD HH:MM", v = height (ft), type = "H" or "L"
        t = p.get("t", "")
        if len(t) < 16:
            continue
        d, hm = t[:10], t[11:16]
        try:
            v = float(p.get("v"))
        except (TypeError, ValueError):
            continue
        by_date[d].append((p.get("type", ""), hm, v))

    rows: dict[str, dict] = {}
    for d, events in by_date.items():
        highs = sorted([(t, v) for kind, t, v in events if kind == "H"])
        lows  = sorted([(t, v) for kind, t, v in events if kind == "L"])
        rec: dict = {"as_of_date": d}
        rec["hi_tide_1_time"], rec["hi_tide_1_height"] = (highs[0] if highs else (None, None))
        rec["hi_tide_2_time"], rec["hi_tide_2_height"] = (highs[1] if len(highs) > 1 else (None, None))
        rec["lo_tide_1_time"], rec["lo_tide_1_height"] = (lows[0]  if lows  else (None, None))
        rec["lo_tide_2_time"], rec["lo_tide_2_height"] = (lows[1]  if len(lows) > 1 else (None, None))

        # Range = max high − min low for the day
        all_h = [v for _, v in highs]
        all_l = [v for _, v in lows]
        if all_h and all_l:
            rec["tide_range_ft"] = round(max(all_h) - min(all_l), 2)
        else:
            rec["tide_range_ft"] = None
        rec["has_negative_low"] = 1 if (all_l and min(all_l) < 0) else 0
        rows[d] = rec
    return rows


def _aggregate_water_temp(observations: list[dict]) -> dict[str, float]:
    """Average hourly water temp readings into a daily mean (°F)."""
    daily: dict[str, list[float]] = defaultdict(list)
    for obs in observations:
        t = obs.get("t", "")
        if len(t) < 10:
            continue
        try:
            v = float(obs.get("v"))
        except (TypeError, ValueError):
            continue
        daily[t[:10]].append(v)
    return {d: round(sum(v) / len(v), 1) for d, v in daily.items() if v}


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    today = date.today()
    start = today - timedelta(days=DAYS_BACK)
    end   = today + timedelta(days=DAYS_FORWARD)

    # Tide predictions (forward-looking, always available)
    try:
        tide_rows = _aggregate_tides(_fetch_noaa("predictions", start, end))
    except Exception as exc:
        print(f"[WARN] NOAA tide predictions failed: {exc}")
        tide_rows = {}

    # Water temperature (observed only — past dates only)
    try:
        wt_obs = _fetch_noaa("water_temperature", start, today)
        wt_daily = _aggregate_water_temp(wt_obs)
    except Exception as exc:
        # Some stations don't report water_temperature; this is non-fatal.
        print(f"[INFO] NOAA water temp not available for {STATION_ID}: {exc}")
        wt_daily = {}

    if not tide_rows:
        print("[WARN] No tide data returned. Nothing to write.")
        conn.close()
        return 0

    rows = []
    for d, rec in sorted(tide_rows.items()):
        rows.append((
            d, STATION_ID, STATION_NAME,
            rec.get("hi_tide_1_time"), rec.get("hi_tide_1_height"),
            rec.get("lo_tide_1_time"), rec.get("lo_tide_1_height"),
            rec.get("hi_tide_2_time"), rec.get("hi_tide_2_height"),
            rec.get("lo_tide_2_time"), rec.get("lo_tide_2_height"),
            rec.get("tide_range_ft"), rec.get("has_negative_low"),
            wt_daily.get(d),
        ))

    conn.executemany(
        """INSERT OR REPLACE INTO noaa_tides_daily
           (as_of_date, station_id, station_name,
            hi_tide_1_time, hi_tide_1_height,
            lo_tide_1_time, lo_tide_1_height,
            hi_tide_2_time, hi_tide_2_height,
            lo_tide_2_time, lo_tide_2_height,
            tide_range_ft, has_negative_low, water_temp_f_avg)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    n = len(rows)
    print(f"[OK]   NOAA tides {STATION_ID}: {n} daily rows ({DAYS_BACK} back + {DAYS_FORWARD} forward)")
    conn.close()
    return n


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_noaa_tides: {n} rows inserted/updated")
    sys.exit(0)
