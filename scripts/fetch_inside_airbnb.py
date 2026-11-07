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
fetch_inside_airbnb.py
-----------------------
Loads InsideAirbnb STVR market summary data for Dana Point from a seeded CSV.

InsideAirbnb (insideairbnb.com) publishes free short-term rental listing
snapshots. Dana Point's data is not independently published — this script
uses a seeded monthly summary for 2024 based on known market characteristics:
  • ~400–500 active listings
  • Average nightly rate: ~$270–$375 (peaks June–August)
  • Average occupancy: ~61–84% (summer peaks, winter valleys)
  • Property mix: ~60% entire home / 30% private room / 10% other

Use for:
  • Hotel vs. STVR rate and occupancy comparison
  • Competitive supply pressure analysis
  • Shoulder-season STVR availability signals

Tables written:
  stvr_market_summary — month, active_listings, ADR, occ_pct, revpar,
                        entire_home_pct, avg_review_score, avg_min_nights
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
DB       = ROOT / "data" / "analytics.sqlite"
CSV_PATH = ROOT / "data" / "inside_airbnb" / "dana_point_stvr_summary.csv"

INIT_SQL = """
CREATE TABLE IF NOT EXISTS stvr_market_summary (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    month             TEXT,
    active_listings   INTEGER,
    avg_daily_rate    REAL,
    occupancy_pct     REAL,
    revpar            REAL,
    entire_home_pct   REAL,
    avg_review_score  REAL,
    avg_min_nights    REAL,
    data_source       TEXT DEFAULT 'InsideAirbnb/seeded',
    inserted_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(month)
);
"""


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    if not CSV_PATH.exists():
        print(f"[{_ts()}] [SKIP] fetch_inside_airbnb — CSV not found: {CSV_PATH}")
        return 0

    conn = sqlite3.connect(DB, timeout=10)
    try:
        conn.execute(INIT_SQL)
        conn.commit()

        df = pd.read_csv(CSV_PATH)
        required = {"month", "active_listings", "avg_daily_rate", "occupancy_pct",
                    "revpar", "entire_home_pct", "avg_review_score", "avg_min_nights"}
        missing = required - set(df.columns)
        if missing:
            print(f"[{_ts()}] [FAIL] fetch_inside_airbnb — missing columns: {missing}")
            conn.close()
            return 0

        numeric_cols = ["active_listings", "avg_daily_rate", "occupancy_pct",
                        "revpar", "entire_home_pct", "avg_review_score", "avg_min_nights"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        rows = [
            (
                str(r.month),
                int(r.active_listings) if pd.notna(r.active_listings) else None,
                float(r.avg_daily_rate)   if pd.notna(r.avg_daily_rate)   else None,
                float(r.occupancy_pct)    if pd.notna(r.occupancy_pct)    else None,
                float(r.revpar)           if pd.notna(r.revpar)           else None,
                float(r.entire_home_pct)  if pd.notna(r.entire_home_pct)  else None,
                float(r.avg_review_score) if pd.notna(r.avg_review_score) else None,
                float(r.avg_min_nights)   if pd.notna(r.avg_min_nights)   else None,
            )
            for r in df.itertuples(index=False)
        ]

        conn.executemany(
            """INSERT OR IGNORE INTO stvr_market_summary
               (month, active_listings, avg_daily_rate, occupancy_pct, revpar,
                entire_home_pct, avg_review_score, avg_min_nights)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        inserted = conn.execute("SELECT changes()").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM stvr_market_summary").fetchone()[0]
        print(
            f"[{_ts()}] [OK]   fetch_inside_airbnb — "
            f"{inserted} rows inserted, {total} total in stvr_market_summary"
        )
    except Exception as exc:
        print(f"[{_ts()}] [FAIL] fetch_inside_airbnb — {exc}")
        conn.close()
        return 0
    conn.close()
    return inserted


if __name__ == "__main__":
    n = main()
    sys.exit(0)
