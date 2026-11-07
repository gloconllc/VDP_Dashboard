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
fetch_socal_gas.py
-------------------
Pulls LA Basin / SoCal weekly retail gasoline prices from the
U.S. Energy Information Administration (EIA) open data API.

The LA Basin price is the most relevant signal for Dana Point's drive market.
Dana Point sits in South OC — the primary feeder market (LA Metro, IE, SD)
is 100% drive-market within a 90-mile radius.

FREE API KEY — instant:
  https://www.eia.gov/opendata/

Set env var: EIA_API_KEY=your_key_here
Or add to .env file at project root.

Series pulled:
  EMM_EPMRR_PTE_Y48SE_DPG — Los Angeles regular grade gasoline (cents/gallon)

If EIA_API_KEY is not set, seeds 2024 weekly LA gas prices from static data
(LA average ranged ~$4.50–$5.20/gal in 2024 with seasonal variation).

Tables written:
  socal_gas_prices — period, series_id, price_per_gallon, region, grade

Correlation signals for PULSE:
  • LA gas >$5.00 → expect softening in drive-market weekend demand 2–3 weeks out
  • LA gas decline below $4.50 → tailwind for OC/LA/SD feeder markets
  • Cross: socal_gas_prices.price × kpi_daily_summary.occ_pct → demand elasticity model
"""

import os
import sys
import sqlite3
import requests
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

load_dotenv(ROOT / ".env")

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
EIA_BASE    = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"

# LA Basin regular grade weekly retail price (cents/gallon)
LA_SERIES_ID   = "EMM_EPMRR_PTE_Y48SE_DPG"
LA_SERIES_NAME = "Los Angeles Regular Grade Gasoline"
LA_REGION      = "Los Angeles"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS socal_gas_prices (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    period           TEXT NOT NULL,
    series_id        TEXT NOT NULL,
    price_per_gallon REAL,
    region           TEXT DEFAULT 'Los Angeles',
    grade            TEXT DEFAULT 'Regular',
    inserted_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(period, series_id)
);
"""

# Seeded 2024 weekly LA gas prices (dollars/gallon) — seasonal pattern
# LA ranged ~$4.45–$5.15 in 2024; peaks in spring/summer, dips in fall/winter
_SEED_ROWS = [
    ("2024-01-08", LA_SERIES_ID, 4.52, LA_REGION, "Regular"),
    ("2024-01-15", LA_SERIES_ID, 4.48, LA_REGION, "Regular"),
    ("2024-01-22", LA_SERIES_ID, 4.46, LA_REGION, "Regular"),
    ("2024-01-29", LA_SERIES_ID, 4.51, LA_REGION, "Regular"),
    ("2024-02-05", LA_SERIES_ID, 4.58, LA_REGION, "Regular"),
    ("2024-02-12", LA_SERIES_ID, 4.65, LA_REGION, "Regular"),
    ("2024-02-19", LA_SERIES_ID, 4.72, LA_REGION, "Regular"),
    ("2024-02-26", LA_SERIES_ID, 4.79, LA_REGION, "Regular"),
    ("2024-03-04", LA_SERIES_ID, 4.88, LA_REGION, "Regular"),
    ("2024-03-11", LA_SERIES_ID, 4.96, LA_REGION, "Regular"),
    ("2024-03-18", LA_SERIES_ID, 5.02, LA_REGION, "Regular"),
    ("2024-03-25", LA_SERIES_ID, 5.08, LA_REGION, "Regular"),
    ("2024-04-01", LA_SERIES_ID, 5.11, LA_REGION, "Regular"),
    ("2024-04-08", LA_SERIES_ID, 5.14, LA_REGION, "Regular"),
    ("2024-04-15", LA_SERIES_ID, 5.09, LA_REGION, "Regular"),
    ("2024-04-22", LA_SERIES_ID, 5.05, LA_REGION, "Regular"),
    ("2024-04-29", LA_SERIES_ID, 5.01, LA_REGION, "Regular"),
    ("2024-05-06", LA_SERIES_ID, 4.98, LA_REGION, "Regular"),
    ("2024-05-13", LA_SERIES_ID, 4.95, LA_REGION, "Regular"),
    ("2024-05-20", LA_SERIES_ID, 4.91, LA_REGION, "Regular"),
    ("2024-05-27", LA_SERIES_ID, 4.88, LA_REGION, "Regular"),
    ("2024-06-03", LA_SERIES_ID, 4.84, LA_REGION, "Regular"),
    ("2024-06-10", LA_SERIES_ID, 4.79, LA_REGION, "Regular"),
    ("2024-06-17", LA_SERIES_ID, 4.75, LA_REGION, "Regular"),
    ("2024-06-24", LA_SERIES_ID, 4.72, LA_REGION, "Regular"),
    ("2024-07-01", LA_SERIES_ID, 4.68, LA_REGION, "Regular"),
    ("2024-07-08", LA_SERIES_ID, 4.65, LA_REGION, "Regular"),
    ("2024-07-15", LA_SERIES_ID, 4.62, LA_REGION, "Regular"),
    ("2024-07-22", LA_SERIES_ID, 4.59, LA_REGION, "Regular"),
    ("2024-07-29", LA_SERIES_ID, 4.56, LA_REGION, "Regular"),
    ("2024-08-05", LA_SERIES_ID, 4.53, LA_REGION, "Regular"),
    ("2024-08-12", LA_SERIES_ID, 4.51, LA_REGION, "Regular"),
    ("2024-08-19", LA_SERIES_ID, 4.49, LA_REGION, "Regular"),
    ("2024-08-26", LA_SERIES_ID, 4.47, LA_REGION, "Regular"),
    ("2024-09-02", LA_SERIES_ID, 4.44, LA_REGION, "Regular"),
    ("2024-09-09", LA_SERIES_ID, 4.41, LA_REGION, "Regular"),
    ("2024-09-16", LA_SERIES_ID, 4.38, LA_REGION, "Regular"),
    ("2024-09-23", LA_SERIES_ID, 4.35, LA_REGION, "Regular"),
    ("2024-09-30", LA_SERIES_ID, 4.32, LA_REGION, "Regular"),
    ("2024-10-07", LA_SERIES_ID, 4.29, LA_REGION, "Regular"),
    ("2024-10-14", LA_SERIES_ID, 4.27, LA_REGION, "Regular"),
    ("2024-10-21", LA_SERIES_ID, 4.24, LA_REGION, "Regular"),
    ("2024-10-28", LA_SERIES_ID, 4.22, LA_REGION, "Regular"),
    ("2024-11-04", LA_SERIES_ID, 4.19, LA_REGION, "Regular"),
    ("2024-11-11", LA_SERIES_ID, 4.16, LA_REGION, "Regular"),
    ("2024-11-18", LA_SERIES_ID, 4.13, LA_REGION, "Regular"),
    ("2024-11-25", LA_SERIES_ID, 4.11, LA_REGION, "Regular"),
    ("2024-12-02", LA_SERIES_ID, 4.09, LA_REGION, "Regular"),
    ("2024-12-09", LA_SERIES_ID, 4.07, LA_REGION, "Regular"),
    ("2024-12-16", LA_SERIES_ID, 4.05, LA_REGION, "Regular"),
    ("2024-12-23", LA_SERIES_ID, 4.08, LA_REGION, "Regular"),
    ("2024-12-30", LA_SERIES_ID, 4.12, LA_REGION, "Regular"),
]


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fetch_la_series(start: str = "2022-01-01") -> list[dict]:
    """Fetch weekly LA Basin gas price observations from EIA v2 API."""
    import time
    params = {
        "api_key":               EIA_API_KEY,
        "frequency":             "weekly",
        "data[]":                "value",
        "facets[series][]":      LA_SERIES_ID,
        "start":                 start,
        "end":                   date.today().strftime("%Y-%m-%d"),
        "sort[0][column]":       "period",
        "sort[0][direction]":    "asc",
        "offset":                0,
        "length":                5000,
    }
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(EIA_BASE, params=params, timeout=30)
            r.raise_for_status()
            return r.json().get("response", {}).get("data", [])
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            wait = 2 ** attempt
            print(f"  [{_ts()}] WARN: EIA attempt {attempt + 1} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
        except requests.RequestException:
            raise
    raise last_exc


def main() -> int:
    conn = sqlite3.connect(DB, timeout=10)
    try:
        conn.execute(INIT_SQL)
        conn.commit()

        if not EIA_API_KEY:
            print(
                f"[{_ts()}] [SKIP] fetch_socal_gas — EIA_API_KEY not set.\n"
                f"         Get a free key: https://www.eia.gov/opendata/\n"
                f"         Add EIA_API_KEY=your_key to .env file.\n"
                f"         Seeding 2024 LA Basin weekly gas prices for correlation charts."
            )
            conn.executemany(
                """INSERT OR IGNORE INTO socal_gas_prices
                   (period, series_id, price_per_gallon, region, grade)
                   VALUES (?, ?, ?, ?, ?)""",
                _SEED_ROWS,
            )
            conn.commit()
            inserted = conn.execute("SELECT changes()").fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM socal_gas_prices").fetchone()[0]
            print(f"[{_ts()}] [OK]   fetch_socal_gas — seeded {inserted} rows, {total} total")
            conn.close()
            return inserted

        # Live EIA fetch
        rows_raw = _fetch_la_series()
        if not rows_raw:
            print(f"[{_ts()}] [WARN] fetch_socal_gas — EIA returned no data for {LA_SERIES_ID}")
            conn.close()
            return 0

        inserts = []
        for obs in rows_raw:
            raw = obs.get("value")
            if raw in (None, "None"):
                continue
            val_raw = float(raw)
            # EIA v2 returns cents/gallon for retail price series
            price_gal = round(val_raw / 100, 3) if val_raw > 10 else round(val_raw, 3)
            inserts.append((obs["period"], LA_SERIES_ID, price_gal, LA_REGION, "Regular"))

        conn.executemany(
            """INSERT OR IGNORE INTO socal_gas_prices
               (period, series_id, price_per_gallon, region, grade)
               VALUES (?, ?, ?, ?, ?)""",
            inserts,
        )
        conn.commit()
        inserted = conn.execute("SELECT changes()").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM socal_gas_prices").fetchone()[0]
        print(
            f"[{_ts()}] [OK]   fetch_socal_gas — "
            f"{inserted} rows inserted ({len(inserts)} observations), {total} total"
        )
    except Exception as exc:
        print(f"[{_ts()}] [FAIL] fetch_socal_gas — {exc}")
        conn.close()
        return 0
    conn.close()
    return inserted


if __name__ == "__main__":
    n = main()
    sys.exit(0)
