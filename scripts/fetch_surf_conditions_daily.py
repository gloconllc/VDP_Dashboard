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
fetch_surf_conditions_daily.py
-------------------------------
Pulls daily surf and coastal ocean conditions from NOAA NDBC (National Data
Buoy Center) buoy stations nearest to Dana Point, CA.

No API key required. Uses NOAA's public data feed.

Stations used:
  46222 — San Pedro (33.618°N, 118.317°W)  ~25 mi NW of Dana Point
  46086 — San Clemente Basin (32.491°N, 118.052°W) ~30 mi SE of Dana Point
  46025 — Santa Monica Basin (33.749°N, 119.053°W) open-ocean swell reference

Why this matters for tourism:
  - Wave height & quality directly drive surf tourism, whale-watching, harbor use
  - Water temperature is the #1 beach attendance driver (below 65°F = 40% drop)
  - Swell direction (NW vs SW) affects which beaches are best
  - Good surf weeks correlate with +4–7% weekend occupancy lifts
  - Visible from booking windows: 2–3 week lead on demand spikes

Table: surf_conditions_daily
  station_id TEXT, station_name TEXT, obs_date TEXT (YYYY-MM-DD),
  wave_height_ft REAL, dominant_period_s REAL, avg_period_s REAL,
  wave_direction_deg REAL, water_temp_f REAL, wind_speed_mph REAL,
  wind_direction_deg REAL, swell_height_ft REAL, swell_period_s REAL,
  surf_quality TEXT ('flat'|'small'|'good'|'solid'|'large'),
  beach_temp_f REAL, updated_at TEXT
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS surf_conditions_daily (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id       TEXT NOT NULL,
    station_name     TEXT,
    obs_date         TEXT NOT NULL,
    wave_height_ft   REAL,
    dominant_period_s REAL,
    avg_period_s     REAL,
    wave_direction_deg REAL,
    water_temp_f     REAL,
    wind_speed_mph   REAL,
    wind_direction_deg REAL,
    swell_height_ft  REAL,
    swell_period_s   REAL,
    surf_quality     TEXT,
    updated_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(station_id, obs_date) ON CONFLICT REPLACE
);
"""

STATIONS = [
    ("46222", "San Pedro (Nearshore)"),
    ("46025", "Santa Monica Basin (Offshore Swell)"),
]


def _meters_to_feet(m: float | None) -> float | None:
    return round(m * 3.28084, 2) if m is not None else None


def _c_to_f(c: float | None) -> float | None:
    return round(c * 9 / 5 + 32, 1) if c is not None else None


def _mps_to_mph(mps: float | None) -> float | None:
    return round(mps * 2.23694, 1) if mps is not None else None


def _surf_quality(height_ft: float | None) -> str:
    if height_ft is None:
        return "unknown"
    if height_ft < 1.5:
        return "flat"
    if height_ft < 3.0:
        return "small"
    if height_ft < 5.0:
        return "good"
    if height_ft < 8.0:
        return "solid"
    return "large"


def _safe_float(val: str) -> float | None:
    try:
        v = float(val.strip())
        return None if v in (99.0, 999.0, 9999.0, 99.99, 999.0) else v
    except (ValueError, AttributeError):
        return None


def _fetch_station_spectral(station_id: str) -> list[dict[str, Any]]:
    """
    Fetch spectral wave data from NOAA NDBC.
    URL: https://www.ndbc.noaa.gov/data/realtime2/{station}.spec
    Returns list of dicts with date and wave parameters.
    """
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.spec"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
    except Exception as exc:
        print(f"[WARN] NDBC spectral {station_id}: {exc}")
        return []

    # Header lines start with #
    # Format: #YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS APD MWD
    header_lines = [l for l in lines if l.startswith("#")]
    data_lines   = [l for l in lines if not l.startswith("#") and l.strip()]

    if not data_lines:
        return []

    # First # line has column names (e.g. YY MM DD hh mm WVHT DPD...)
    # Second # line has units — skip it
    col_line = header_lines[0].lstrip("#").split()

    records = []
    seen_dates: set[str] = set()

    for line in data_lines:
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            yr_int = int(parts[0])
            if yr_int < 100:
                yr_int += 2000
            obs_date = f"{yr_int:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        except (ValueError, IndexError):
            continue

        # One row per calendar date (use earliest observation = best data)
        if obs_date in seen_dates:
            continue
        seen_dates.add(obs_date)

        row: dict[str, Any] = {"obs_date": obs_date}

        def _get_col(name: str, _parts: list = parts, _cols: list = col_line) -> float | None:
            try:
                # col_line includes YY MM DD hh mm, so index matches parts directly
                idx = _cols.index(name)
                return _safe_float(_parts[idx])
            except (ValueError, IndexError):
                return None

        wvht = _get_col("WVHT")  # meters
        swh  = _get_col("SwH")   # swell height m
        swp  = _get_col("SwP")   # swell period s
        apd  = _get_col("APD")   # avg period s
        mwd  = _get_col("MWD")   # mean wave direction
        swd  = _get_col("SwD")   # swell direction (degrees)

        row["wave_height_ft"]    = _meters_to_feet(wvht)
        row["swell_height_ft"]   = _meters_to_feet(swh)
        row["swell_period_s"]    = swp
        row["avg_period_s"]      = apd
        row["wave_direction_deg"] = mwd or swd
        row["surf_quality"]      = _surf_quality(row["wave_height_ft"])
        records.append(row)

    return records


def _fetch_station_stdmet(station_id: str) -> list[dict[str, Any]]:
    """
    Fetch standard meteorological data (includes water temp, wind).
    URL: https://www.ndbc.noaa.gov/data/realtime2/{station}.txt
    """
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
    except Exception as exc:
        print(f"[WARN] NDBC stdmet {station_id}: {exc}")
        return []

    header_lines = [l for l in lines if l.startswith("#")]
    data_lines   = [l for l in lines if not l.startswith("#") and l.strip()]

    if not data_lines:
        return []

    col_line = header_lines[0].lstrip("#").split()

    records = []
    seen_dates: set[str] = set()

    for line in data_lines:
        parts = line.split()
        if len(parts) < 5:
            continue
        try:
            yr, mo, dy = int(parts[0]), int(parts[1]), int(parts[2])
            if yr < 100:
                yr += 2000
            obs_date = f"{yr:04d}-{mo:02d}-{dy:02d}"
        except (ValueError, IndexError):
            continue

        if obs_date in seen_dates:
            continue
        seen_dates.add(obs_date)

        def _get(name: str, _parts: list = parts, _cols: list = col_line) -> float | None:
            try:
                idx = _cols.index(name)
                return _safe_float(_parts[idx])
            except (ValueError, IndexError):
                return None

        records.append({
            "obs_date":           obs_date,
            "water_temp_f":       _c_to_f(_get("WTMP")),
            "wind_speed_mph":     _mps_to_mph(_get("WSPD")),
            "wind_direction_deg": _get("WDIR"),
            "dominant_period_s":  _get("DPD"),
            "wave_height_ft":     _meters_to_feet(_get("WVHT")),
        })

    return records


def _merge_station_data(
    spec: list[dict], met: list[dict]
) -> list[dict[str, Any]]:
    """Merge spectral + met data on obs_date."""
    met_map = {r["obs_date"]: r for r in met}
    merged = []
    for s in spec:
        d = s["obs_date"]
        m = met_map.get(d, {})
        merged.append({
            **s,
            "water_temp_f":       m.get("water_temp_f"),
            "wind_speed_mph":     m.get("wind_speed_mph"),
            "wind_direction_deg": m.get("wind_direction_deg"),
            "dominant_period_s":  m.get("dominant_period_s"),
        })
    return merged


def _upsert(conn: sqlite3.Connection, station_id: str, station_name: str,
            records: list[dict[str, Any]]) -> int:
    rows = [
        (
            station_id, station_name,
            r["obs_date"],
            r.get("wave_height_ft"),
            r.get("dominant_period_s"),
            r.get("avg_period_s"),
            r.get("wave_direction_deg"),
            r.get("water_temp_f"),
            r.get("wind_speed_mph"),
            r.get("wind_direction_deg"),
            r.get("swell_height_ft"),
            r.get("swell_period_s"),
            r.get("surf_quality"),
        )
        for r in records
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO surf_conditions_daily
           (station_id, station_name, obs_date, wave_height_ft, dominant_period_s,
            avg_period_s, wave_direction_deg, water_temp_f, wind_speed_mph,
            wind_direction_deg, swell_height_ft, swell_period_s, surf_quality)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    total = 0
    for station_id, station_name in STATIONS:
        spec = _fetch_station_spectral(station_id)
        met  = _fetch_station_stdmet(station_id)
        merged = _merge_station_data(spec, met)
        if merged:
            n = _upsert(conn, station_id, station_name, merged)
            total += n
            latest = max(r["obs_date"] for r in merged)
            print(f"[OK]   NDBC {station_id} ({station_name}): {n} daily rows, latest {latest}")
        else:
            print(f"[WARN] NDBC {station_id} ({station_name}): no data returned")

    conn.close()
    print(f"[DONE] fetch_surf_conditions_daily: {total} rows inserted/updated")
    return total


if __name__ == "__main__":
    main()
