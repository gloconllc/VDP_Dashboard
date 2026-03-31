"""
fetch_bls_data.py
-----------------
Pulls employment data for the Leisure & Hospitality sector from the U.S.
Bureau of Labor Statistics (BLS) public API.

This is the employment context layer used by CoStar, STR supplementary
reports, and Symphony (Tourism Economics) to frame hotel performance within
the broader labor market and sector health.

API Tiers:
  v1 (no key): 500 queries/day, single-series only, limited date range
  v2 (with key): 50 queries/day (multi-series), 10-year range, YoY available
  Register free at: https://data.bls.gov/registrationEngine/

Set env var: BLS_API_KEY=your_key_here  (optional but recommended)

Series pulled:
  CEU7000000001  — US Leisure & Hospitality, All Employees, SA (national)
  CEU7072200001  — US Accommodation & Food Services, All Employees, SA
  SMU0604200070000000 01 — CA OC-Anaheim Leisure & Hospitality (state/metro)

Table written:
  bls_employment_monthly — series_id, series_name, geo, year, month, value_thousands, yoy_chg_pct
"""

import os
import sys
import json
import sqlite3
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DB   = ROOT / "data" / "analytics.sqlite"

BLS_API_KEY = os.getenv("BLS_API_KEY", "")
BLS_V2_URL  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# Series: id → (display_name, geo_label)
SERIES = {
    "CEU7000000001":   ("US Leisure & Hospitality Employment",         "National"),
    "CEU7072200001":   ("US Accommodation & Food Services Employment", "National"),
    "SMU06000007000000001": ("CA Leisure & Hospitality Employment",    "California"),
    "SMU06000007072200001": ("CA Accommodation Employment",            "California"),
}

INIT_SQL = """
CREATE TABLE IF NOT EXISTS bls_employment_monthly (
    series_id       TEXT    NOT NULL,
    series_name     TEXT,
    geo             TEXT,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    value_thousands REAL,
    yoy_chg_pct     REAL,
    updated_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(series_id, year, month) ON CONFLICT REPLACE
);
"""

CURRENT_YEAR = datetime.now().year
START_YEAR   = CURRENT_YEAR - 6   # BLS v1 allows up to 10 years back


def _fetch_v2(series_ids: list[str]) -> dict:
    """Use BLS v2 API (multi-series). Requires API key for best results."""
    payload = {
        "seriesid":  series_ids,
        "startyear": str(START_YEAR),
        "endyear":   str(CURRENT_YEAR),
        "annualaverage": False,
    }
    if BLS_API_KEY:
        payload["registrationkey"] = BLS_API_KEY

    r = requests.post(
        BLS_V2_URL,
        data=json.dumps(payload),
        headers={"Content-type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _fetch_v1_single(series_id: str) -> dict:
    """Fallback: BLS v1 public API — single series at a time."""
    url = (
        f"https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}"
        f"?startyear={START_YEAR}&endyear={CURRENT_YEAR}"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def _parse_and_store(results_payload: dict, conn: sqlite3.Connection) -> int:
    series_list = (
        results_payload.get("Results", {}).get("series", [])
        or results_payload.get("Results", {}).get("series", [])
    )
    total = 0

    for series_obj in series_list:
        sid  = series_obj.get("seriesID", "")
        name, geo = SERIES.get(sid, (sid, "Unknown"))
        raw_data = series_obj.get("data", [])

        # Parse into (year, month, value) tuples
        obs_list = []
        for obs in raw_data:
            try:
                month_str = obs.get("period", "M00")
                if not month_str.startswith("M") or month_str == "M13":
                    continue  # skip annual averages
                month = int(month_str[1:])
                year  = int(obs["year"])
                val   = float(obs["value"])
                obs_list.append((year, month, val))
            except (KeyError, ValueError):
                continue

        obs_list.sort(key=lambda x: (x[0], x[1]))

        # Compute YoY change
        val_map = {(y, m): v for y, m, v in obs_list}
        for year, month, val in obs_list:
            prior_year_key = (year - 1, month)
            prior_val      = val_map.get(prior_year_key)
            yoy = None
            if prior_val and prior_val != 0:
                yoy = round((val - prior_val) / prior_val * 100, 2)

            conn.execute(
                """INSERT OR REPLACE INTO bls_employment_monthly
                   (series_id, series_name, geo, year, month, value_thousands, yoy_chg_pct)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (sid, name, geo, year, month, val, yoy),
            )
            total += 1

        print(f"[OK]   BLS {sid} ({name}): {len(obs_list)} months")

    conn.commit()
    return total


def main() -> int:
    conn = sqlite3.connect(DB)
    conn.execute(INIT_SQL)
    conn.commit()

    series_ids = list(SERIES.keys())
    total = 0

    # Try v2 multi-series first
    try:
        payload = _fetch_v2(series_ids)
        if payload.get("status") == "REQUEST_SUCCEEDED":
            total = _parse_and_store(payload, conn)
        else:
            raise ValueError(f"BLS API status: {payload.get('status')} — {payload.get('message', [])}")
    except Exception as exc:
        print(f"[WARN] BLS v2 failed ({exc}), falling back to v1 single-series requests")
        # Fallback: pull each series individually
        for sid in series_ids:
            try:
                payload = _fetch_v1_single(sid)
                # Wrap in same structure as v2 for unified parser
                wrapped = {"Results": {"series": [{"seriesID": sid, "data": payload.get("Results", {}).get("series", [{}])[0].get("data", [])}]}}
                total += _parse_and_store(wrapped, conn)
            except Exception as e2:
                print(f"[WARN] BLS v1 {sid} failed: {e2}")

    conn.close()
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_bls_data: {n} rows inserted/updated")
    sys.exit(0)
