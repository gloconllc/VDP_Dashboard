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
fetch_bts_routes.py
--------------------
Loads BTS T-100 Domestic Segment route passenger data for SoCal airports
(SNA/John Wayne, SAN/San Diego, LAX) from a seeded CSV.

BTS T-100 data is published by the Bureau of Transportation Statistics at:
  https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMF

Direct download requires form submission, so this script reads a pre-seeded
CSV at data/bts/bts_t100_routes.csv representing approximate 2024 annual
passenger volumes for the top feeder markets Dana Point cares about.

Use for:
  • Feeder market air connectivity analysis
  • Cross-reference with Datafy DMA visitor share vs. seat capacity
  • Fly-market vs. drive-market demand decomposition

Tables written:
  bts_route_passengers — year, quarter, origin→dest, carrier, pax, seats,
                         load_factor_pct, avg_fare_usd
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
DB      = ROOT / "data" / "analytics.sqlite"
CSV_PATH = ROOT / "data" / "bts" / "bts_t100_routes.csv"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS bts_route_passengers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    year                INTEGER,
    quarter             INTEGER,
    origin_city         TEXT,
    origin_state        TEXT,
    dest_airport_code   TEXT,
    dest_city           TEXT,
    carrier             TEXT,
    passengers          INTEGER,
    seats               INTEGER,
    load_factor_pct     REAL,
    avg_fare_usd        REAL,
    inserted_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(year, quarter, origin_city, dest_airport_code, carrier)
);
"""


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    if not CSV_PATH.exists():
        print(f"[{_ts()}] [SKIP] fetch_bts_routes — CSV not found: {CSV_PATH}")
        return 0

    conn = sqlite3.connect(DB, timeout=10)
    try:
        conn.execute(INIT_SQL)
        conn.commit()

        df = pd.read_csv(CSV_PATH)
        required = {"year", "quarter", "origin_city", "origin_state",
                    "dest_airport_code", "dest_city", "carrier",
                    "passengers", "seats", "departures"}
        missing = required - set(df.columns)
        if missing:
            print(f"[{_ts()}] [FAIL] fetch_bts_routes — missing columns: {missing}")
            conn.close()
            return 0

        # Compute load factor from seats/passengers
        df["passengers"] = pd.to_numeric(df["passengers"], errors="coerce").fillna(0).astype(int)
        df["seats"]       = pd.to_numeric(df["seats"],      errors="coerce").fillna(0).astype(int)
        df["avg_fare_usd"] = pd.to_numeric(df.get("avg_fare_usd"), errors="coerce")
        df["load_factor_pct"] = (
            (df["passengers"] / df["seats"].replace(0, pd.NA)) * 100
        ).round(2)

        rows = [
            (
                int(r.year),
                int(r.quarter),
                str(r.origin_city),
                str(r.origin_state),
                str(r.dest_airport_code),
                str(r.dest_city),
                str(r.carrier),
                int(r.passengers),
                int(r.seats),
                float(r.load_factor_pct) if pd.notna(r.load_factor_pct) else None,
                float(r.avg_fare_usd)    if pd.notna(r.avg_fare_usd)    else None,
            )
            for r in df.itertuples(index=False)
        ]

        conn.executemany(
            """INSERT OR IGNORE INTO bts_route_passengers
               (year, quarter, origin_city, origin_state, dest_airport_code,
                dest_city, carrier, passengers, seats, load_factor_pct, avg_fare_usd)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        inserted = conn.execute("SELECT changes()").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM bts_route_passengers").fetchone()[0]
        print(
            f"[{_ts()}] [OK]   fetch_bts_routes — "
            f"{inserted} rows inserted, {total} total in bts_route_passengers"
        )
    except Exception as exc:
        print(f"[{_ts()}] [FAIL] fetch_bts_routes — {exc}")
        conn.close()
        return 0
    conn.close()
    return inserted


if __name__ == "__main__":
    n = main()
    sys.exit(0)
