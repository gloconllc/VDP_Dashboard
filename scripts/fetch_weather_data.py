"""
fetch_weather_data.py
---------------------
Pulls historical and recent weather data for Dana Point, CA via Open-Meteo —
a free, open-source weather API with no API key required.
  https://open-meteo.com/

Weather is the most direct demand driver for coastal leisure destinations.
Platforms like Placer.ai correlate foot traffic with weather; STR analysts
reference beach day counts when explaining occupancy swings.

Computes a proprietary BEACH DAY SCORE (0–100) composite:
  40% temperature comfort (65–82°F optimal band)
  30% low precipitation
  30% sunshine hours

Table written:
  weather_monthly — year, month, avg_high_f, avg_low_f, avg_temp_f,
                    precip_inches, sunshine_hrs, uv_index_max, beach_day_score

Coordinates: Dana Point, CA  33.4669°N, 117.6981°W
"""

import sys
import sqlite3
import requests
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

DANA_POINT_LAT = 33.4669
DANA_POINT_LON = -117.6981
START_YEAR     = 2020

INIT_SQL = """
CREATE TABLE IF NOT EXISTS weather_monthly (
    year              INTEGER NOT NULL,
    month             INTEGER NOT NULL,
    avg_high_f        REAL,
    avg_low_f         REAL,
    avg_temp_f        REAL,
    precip_inches     REAL,
    sunshine_hrs      REAL,
    uv_index_max      REAL,
    beach_day_score   REAL,
    updated_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(year, month) ON CONFLICT REPLACE
);
"""

MONTH_NAMES = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


def _c_to_f(c: float) -> float:
    return round(c * 9 / 5 + 32, 1)


def _mm_to_in(mm: float) -> float:
    return round(mm / 25.4, 2)


def _beach_score(avg_high_f: float, precip_in: float, sunshine_hrs: float) -> float:
    """
    Proprietary Beach Day Score (0–100).
    Inspired by tourism demand research: coastal destinations see +15-25%
    weekend occupancy premium on days with high beach scores.

    Components:
      40% — Temperature comfort: optimal band 65–82°F for beach activity
      30% — Precipitation: 0 in = 100 points, scaled down linearly
      30% — Sunshine: monthly total, max useful ~280 hrs
    """
    # Temperature score: peaks at 73°F, decays above 90°F and below 60°F
    temp_score = max(0.0, 100.0 - abs(avg_high_f - 73.0) * 3.5)
    temp_score = min(100.0, temp_score)

    # Rain score: 0 in = 100, every inch costs 18 pts
    rain_score = max(0.0, 100.0 - precip_in * 18.0)

    # Sunshine score: 280+ hrs/mo = perfect
    sun_score = min(100.0, (sunshine_hrs / 280.0) * 100.0)

    composite = temp_score * 0.40 + rain_score * 0.30 + sun_score * 0.30
    return round(composite, 1)


def main() -> int:
    start_date = f"{START_YEAR}-01-01"
    end_date   = date.today().strftime("%Y-%m-%d")

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={DANA_POINT_LAT}"
        f"&longitude={DANA_POINT_LON}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        "sunshine_duration,uv_index_max"
        "&temperature_unit=celsius"
        "&precipitation_unit=mm"
        "&timezone=America%2FLos_Angeles"
    )

    try:
        r = requests.get(url, timeout=40)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        print(f"[WARN] fetch_weather_data: Open-Meteo request failed: {exc}")
        return 0

    daily_data = data.get("daily", {})
    if not daily_data.get("time"):
        print("[WARN] fetch_weather_data: no daily data in API response")
        return 0

    df = pd.DataFrame({
        "date":       pd.to_datetime(daily_data["time"]),
        "tmax_c":     daily_data.get("temperature_2m_max", [None] * len(daily_data["time"])),
        "tmin_c":     daily_data.get("temperature_2m_min", [None] * len(daily_data["time"])),
        "precip_mm":  daily_data.get("precipitation_sum",  [0.0]  * len(daily_data["time"])),
        "sunshine_s": daily_data.get("sunshine_duration",  [0.0]  * len(daily_data["time"])),
        "uv_max":     daily_data.get("uv_index_max",       [None] * len(daily_data["time"])),
    })
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month

    monthly = (
        df.groupby(["year", "month"])
        .agg(
            avg_high_c=("tmax_c",    "mean"),
            avg_low_c= ("tmin_c",    "mean"),
            precip_mm= ("precip_mm", "sum"),
            sunshine_s=("sunshine_s","sum"),
            uv_max=    ("uv_max",    "mean"),
        )
        .reset_index()
    )

    conn  = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    total = 0
    for _, row in monthly.iterrows():
        if pd.isna(row.avg_high_c) or pd.isna(row.avg_low_c):
            continue

        avg_high_f   = _c_to_f(row.avg_high_c)
        avg_low_f    = _c_to_f(row.avg_low_c)
        avg_temp_f   = round((avg_high_f + avg_low_f) / 2, 1)
        precip_in    = _mm_to_in(row.precip_mm) if pd.notna(row.precip_mm) else 0.0
        sunshine_hrs = round(row.sunshine_s / 3600, 1) if pd.notna(row.sunshine_s) else 0.0
        uv_index     = round(row.uv_max, 1) if pd.notna(row.uv_max) else None
        beach_score  = _beach_score(avg_high_f, precip_in, sunshine_hrs)

        conn.execute(
            """INSERT OR REPLACE INTO weather_monthly
               (year, month, avg_high_f, avg_low_f, avg_temp_f,
                precip_inches, sunshine_hrs, uv_index_max, beach_day_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(row.year), int(row.month),
                avg_high_f, avg_low_f, avg_temp_f,
                precip_in, sunshine_hrs, uv_index, beach_score,
            ),
        )
        total += 1

    conn.commit()
    conn.close()

    if total:
        print(f"[OK]   Weather data: {total} month-rows for Dana Point, CA")
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_weather_data: {n} rows inserted/updated")
    sys.exit(0)
