"""
fetch_costar_data.py
---------------------
Loads CoStar market data exports from the downloads/ folder into SQLite.

CoStar does not offer a public API — data arrives as Excel (.xlsx) exports
downloaded manually from costar.com or via CoStar Market Analytics.

Expected file pattern: downloads/costar_*.xlsx

Column mapping (CoStar export → fact_str_metrics):
  Date          → asofdate
  Market        → market
  Submarket     → submarket   (if present)
  Occupancy     → occ         (decimal: 0.688)
  ADR           → adr         (USD)
  RevPAR        → revpar      (USD)
  Supply        → supply      (room-nights)
  Demand        → demand      (room-nights)

Run:
    python3 scripts/fetch_costar_data.py

To add a new CoStar export:
    1. Download the Excel from costar.com → save to downloads/costar_YYYY-MM.xlsx
    2. Run this script (or click "Fetch External Data" in the dashboard)
"""

import os
import sys
import sqlite3
import glob
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DB_PATH      = PROJECT_ROOT / "data" / "analytics.sqlite"
DOWNLOADS    = PROJECT_ROOT / "downloads"
LOG_PATH     = PROJECT_ROOT / "logs" / "pipeline.log"

SOURCE = "costar"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(step: str, status: str, message: str) -> None:
    line = f"{_now()} | {step:<20} | {status:<4} | {message}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as fh:
        fh.write(line + "\n")

# ---------------------------------------------------------------------------
# Column normaliser
# ---------------------------------------------------------------------------

COLUMN_MAP = {
    # CoStar export column name variations → canonical metric name
    "occupancy":          "occ",
    "occ":                "occ",
    "occ rate":           "occ",
    "occupancy rate":     "occ",
    "adr":                "adr",
    "average daily rate": "adr",
    "revpar":             "revpar",
    "rev par":            "revpar",
    "revenue per available room": "revpar",
    "supply":             "supply",
    "demand":             "demand",
    "revenue":            "revenue",
    "room revenue":       "revenue",
}

DATE_COLS  = {"date", "period", "month", "as_of_date", "asofdate"}
MKT_COLS   = {"market", "market name"}
SUBMKT_COLS = {"submarket", "submarket name"}


def _find_column(df_cols: list, candidates: set) -> Optional[str]:
    """Return the first df column whose lowercased name is in candidates."""
    for c in df_cols:
        if c.strip().lower() in candidates:
            return c
    return None


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_file(path: str, conn: sqlite3.Connection) -> int:
    """Parse one CoStar Excel export and insert rows into fact_str_metrics."""
    log("costar_load", "OK  ", f"Reading {Path(path).name}")

    try:
        # Try each sheet; CoStar usually puts data on the first sheet
        df_raw = pd.read_excel(path, sheet_name=0, header=None)
    except Exception as exc:
        log("costar_load", "FAIL", f"Cannot read file: {exc}")
        return 0

    # Auto-detect header row (first row with ≥3 non-null cells)
    header_row = 0
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            header_row = i
            break

    df = pd.read_excel(path, sheet_name=0, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # Identify key columns
    date_col   = _find_column(df.columns.tolist(), DATE_COLS)
    mkt_col    = _find_column(df.columns.tolist(), MKT_COLS)
    submkt_col = _find_column(df.columns.tolist(), SUBMKT_COLS)

    if date_col is None:
        log("costar_load", "FAIL",
            f"No date column found in {Path(path).name}. "
            f"Columns: {list(df.columns)[:10]}")
        return 0

    # Melt metric columns
    metric_cols = {}
    for col in df.columns:
        canonical = COLUMN_MAP.get(col.strip().lower())
        if canonical:
            metric_cols[col] = canonical

    if not metric_cols:
        log("costar_load", "WARN",
            f"No recognised metric columns in {Path(path).name}. "
            f"Columns: {list(df.columns)[:10]}")
        return 0

    rows_inserted = 0
    cursor = conn.cursor()

    for _, row in df.iterrows():
        raw_date = row.get(date_col)
        if pd.isna(raw_date):
            continue
        try:
            asofdate = pd.to_datetime(raw_date).strftime("%Y-%m-%d")
        except Exception:
            continue

        market    = str(row[mkt_col]).strip()    if mkt_col    else "Dana Point"
        submarket = str(row[submkt_col]).strip() if submkt_col else ""

        for col, metric_name in metric_cols.items():
            raw_val = row.get(col)
            value   = pd.to_numeric(raw_val, errors="coerce")
            if pd.isna(value):
                continue

            # CoStar occupancy comes as percentage (68.8) — normalise to decimal
            if metric_name == "occ" and value > 1:
                value = value / 100.0

            unit = {
                "occ":     "decimal",
                "adr":     "USD",
                "revpar":  "USD",
                "revenue": "USD",
                "supply":  "room-nights",
                "demand":  "room-nights",
            }.get(metric_name, "")

            cursor.execute(
                """
                INSERT OR REPLACE INTO fact_str_metrics
                  (source, grain, propertyname, market, submarket,
                   as_of_date, metric_name, metric_value, unit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (SOURCE, "monthly", "", market, submarket,
                 asofdate, metric_name, float(value), unit),
            )
            rows_inserted += 1

    conn.commit()
    return rows_inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    pattern = str(DOWNLOADS / "costar_*.xlsx")
    files   = sorted(glob.glob(pattern))

    if not files:
        log("fetch_costar", "WARN",
            f"No CoStar files found matching downloads/costar_*.xlsx — "
            f"download an export from costar.com and save it to downloads/")
        # Exit 0 so the pipeline doesn't abort — no file is a non-fatal condition
        sys.exit(0)

    conn = sqlite3.connect(str(DB_PATH))
    total = 0
    for f in files:
        n = load_file(f, conn)
        log("fetch_costar", "OK  " if n > 0 else "WARN",
            f"{Path(f).name} → {n} rows inserted")
        total += n

    conn.close()
    log("fetch_costar", "OK  ", f"Done. Total CoStar rows inserted: {total}")


if __name__ == "__main__":
    main()
