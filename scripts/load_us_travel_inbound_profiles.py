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
load_us_travel_inbound_profiles.py — U.S. Dept of Commerce ITA Inbound Market Profile loader

Parses data/us_travel/Inbound-Market-Profile-*.xlsx — National Travel and
Tourism Office overseas-visitor activity profiles (Air, Amusement-Theme-Park,
Car-Rental, Fine-Dining, Hotel-Motel, Shopping, Vacation) — into
us_travel_inbound_market_profile.

Each source file is a large (500+ row) narrative report with dozens of
sub-tables; this loader extracts only the "Visitation Trends" headline
time series (1997-present: total overseas visitor arrivals vs. category
participant volume) that is common to every file. Layer 2 national
context — never overrides Layer 1 STR/Datafy local truth.
"""

import os
import re
from datetime import datetime

import pandas as pd
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
US_TRAVEL_DIR = os.path.join(PROJECT_ROOT, "data", "us_travel")

# filename fragment -> friendly category label
CATEGORY_MAP = {
    "inbound-market-profile-air": "Air",
    "inbound-market-profile-amusement-theme-park": "Amusement-Theme-Park",
    "inbound-market-profile-car-rental": "Car-Rental",
    "inbound-market-profile-fine-dining": "Fine-Dining",
    "inbound-market-profile-hotel-motel": "Hotel-Motel",
    "inbound-market-profile-shopping": "Shopping",
    "inbound-market-profile-vacation": "Vacation",
}

DDL = """
CREATE TABLE IF NOT EXISTS us_travel_inbound_market_profile (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    category                    TEXT NOT NULL,
    year                        INTEGER NOT NULL,
    total_overseas_arrivals_000s REAL,
    category_visitors_000s      REAL,
    pct_change_yoy               REAL,
    loaded_at                    TEXT DEFAULT (datetime('now')),
    UNIQUE(category, year) ON CONFLICT REPLACE
);
"""


def ts() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def _f(val):
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None


def _parse_file(path: str, category: str) -> list:
    df = pd.read_excel(path, sheet_name=0, header=None)

    header_row = None
    for r in range(len(df)):
        cell = df.iloc[r, 1] if df.shape[1] > 1 else None
        if isinstance(cell, str) and "visitation trends" in cell.lower():
            header_row = r
            break
    if header_row is None:
        print(f"{ts()} [WARN] {os.path.basename(path)}: 'Visitation Trends' section not found")
        return []

    # Layout relative to the header row: +3 years, +4 total overseas, +5 category, +6 pct change
    years_row = df.iloc[header_row + 3]
    total_row = df.iloc[header_row + 4]
    category_row = df.iloc[header_row + 5]
    pct_row = df.iloc[header_row + 6]

    rows = []
    for col_idx, yr_val in years_row.items():
        try:
            year = int(yr_val)
        except (TypeError, ValueError):
            continue
        if not (1990 <= year <= 2035):
            continue
        rows.append({
            "category": category,
            "year": year,
            "total_overseas_arrivals_000s": _f(total_row.get(col_idx)),
            "category_visitors_000s": _f(category_row.get(col_idx)),
            "pct_change_yoy": _f(pct_row.get(col_idx)),
        })
    return rows


def main():
    print(f"{ts()} [START] load_us_travel_inbound_profiles.py")
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript(DDL)
    conn.commit()
    cur = conn.cursor()

    total_rows = 0
    files_loaded = 0
    for fname in sorted(os.listdir(US_TRAVEL_DIR)):
        stem_lc = os.path.splitext(fname)[0].lower()
        category = None
        for frag, label in CATEGORY_MAP.items():
            if stem_lc == frag or stem_lc.startswith(frag):
                category = label
                break
        if category is None:
            continue

        path = os.path.join(US_TRAVEL_DIR, fname)
        try:
            rows = _parse_file(path, category)
        except Exception as exc:
            print(f"{ts()} [WARN] {fname}: parse error: {exc}")
            continue

        for r in rows:
            cur.execute(
                """
                INSERT INTO us_travel_inbound_market_profile
                    (category, year, total_overseas_arrivals_000s, category_visitors_000s, pct_change_yoy)
                VALUES (?,?,?,?,?)
                """,
                (r["category"], r["year"], r["total_overseas_arrivals_000s"],
                 r["category_visitors_000s"], r["pct_change_yoy"]),
            )
        conn.commit()
        print(f"{ts()} [OK  ] {fname} -> {category}: {len(rows)} years")
        total_rows += len(rows)
        files_loaded += 1

    conn.execute(
        "INSERT INTO load_log (source, grain, file_name, rows_inserted, run_at) "
        "VALUES ('US_Travel_Inbound_Profiles', 'annual', ?, ?, datetime('now'))",
        (f"Inbound-Market-Profile-*.xlsx ({files_loaded} files)", total_rows),
    )
    conn.commit()
    conn.close()
    print(f"{ts()} [DONE] load_us_travel_inbound_profiles.py: {files_loaded} files, {total_rows} rows")


if __name__ == "__main__":
    main()
